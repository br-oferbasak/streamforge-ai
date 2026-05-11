package ai.streamforge.processor.sink;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds and attaches an Apache Iceberg sink to a {@link UserEventCount} stream.
 *
 * <p>Supported catalog types: {@code hadoop} (default), {@code hive}, {@code rest}.
 * For MinIO / S3-compatible storage set the {@code ICEBERG_S3_*} environment variables
 * so the Hadoop S3A filesystem is configured automatically.
 *
 * <p>Write file size and compaction tuning (via environment variables):
 * <ul>
 *   <li>{@code ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES} — target data file size before rolling,
 *       default {@code 134217728} (128 MB). Smaller files mean more frequent commits but
 *       finer partitioning granularity; larger files reduce S3 PUT costs but slow down
 *       compaction scans.</li>
 *   <li>{@code ICEBERG_WRITE_FORMAT}                 — {@code parquet} (default), {@code avro},
 *       or {@code orc}.</li>
 *   <li>{@code ICEBERG_WRITE_UPSERT}                 — enable upsert mode, default {@code false}.
 *       Requires an equality-delete scheme; trades write amplification for MERGE semantics.</li>
 *   <li>{@code ICEBERG_COMPACTION_TARGET_FILE_SIZE}  — target file size for the rewrite action
 *       (minor compaction), default same as write target. Governs how small files are merged
 *       when running {@code RewriteDataFilesAction}.</li>
 *   <li>{@code ICEBERG_COMPACTION_MIN_INPUT_FILES}   — minimum number of small files that must
 *       exist before a compaction group is submitted, default {@code 5}. Lower values compact
 *       more aggressively; higher values reduce Iceberg catalog write amplification.</li>
 *   <li>{@code ICEBERG_S3_MULTIPART_SIZE}            — S3A multipart upload part size,
 *       default {@code 67108864} (64 MB). Must be ≥ 5 MB (AWS minimum). Larger parts improve
 *       PUT throughput for big files at the cost of memory per upload thread.</li>
 *   <li>{@code ICEBERG_S3_MULTIPART_THRESHOLD}       — file size above which multipart upload
 *       is used, default {@code 67108864} (64 MB).</li>
 * </ul>
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "user_id",         Types.StringType.get()),
            Types.NestedField.required(2, "event_count",     Types.LongType.get()),
            Types.NestedField.required(3, "window_start_ms", Types.LongType.get()),
            Types.NestedField.required(4, "window_end_ms",   Types.LongType.get())
    );

    /**
     * Converts {@code stream} to Iceberg {@link RowData} and appends it to the
     * specified Iceberg table, creating the namespace and table if absent.
     */
    public static void attach(
            DataStream<UserEventCount> stream,
            String catalogType,
            String warehouse,
            String database,
            String tableName,
            String s3Endpoint,
            String s3AccessKey,
            String s3SecretKey) {

        Configuration hadoopConf = buildHadoopConf(s3Endpoint, s3AccessKey, s3SecretKey);
        CatalogLoader catalogLoader = buildCatalogLoader(catalogType, warehouse, hadoopConf);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        ensureTable(catalogLoader, tableId, database);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        DataStream<RowData> rowData = stream
                .map((MapFunction<UserEventCount, RowData>) e -> {
                    GenericRowData row = new GenericRowData(4);
                    row.setField(0, StringData.fromString(e.userId));
                    row.setField(1, e.count);
                    row.setField(2, e.windowStartMs);
                    row.setField(3, e.windowEndMs);
                    return row;
                })
                .returns(TypeInformation.of(RowData.class))
                .name("Map to Iceberg RowData");

        Map<String, String> writeProps = buildWriteProperties();

        FlinkSink.forRowData(rowData)
                .tableLoader(tableLoader)
                .writeParallelism(writeParallelism())
                .setAll(writeProps)
                .append()
                .name("Iceberg Sink: " + database + "." + tableName);

        LOG.info("Iceberg sink attached: catalog={}, warehouse={}, table={}.{}, writeProps={}",
                catalogType, warehouse, database, tableName, writeProps);
    }

    private static CatalogLoader buildCatalogLoader(
            String catalogType, String warehouse, Configuration hadoopConf) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        return switch (catalogType.toLowerCase()) {
            case "hadoop" -> CatalogLoader.hadoop("streamforge", hadoopConf, props);
            case "hive"   -> CatalogLoader.hive("streamforge", hadoopConf, props);
            case "rest"   -> {
                props.put("uri", warehouse);
                yield CatalogLoader.rest("streamforge", hadoopConf, props);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Iceberg catalog type: " + catalogType
                    + ". Supported: hadoop, hive, rest");
        };
    }

    private static void ensureTable(
            CatalogLoader catalogLoader, TableIdentifier tableId, String database) {
        Catalog catalog = catalogLoader.loadCatalog();
        Namespace ns = Namespace.of(database);
        if (!catalog.namespaceExists(ns)) {
            catalog.createNamespace(ns);
        }
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, TABLE_SCHEMA, PartitionSpec.unpartitioned());
            LOG.info("Created Iceberg table {}", tableId);
        }
    }

    private static Configuration buildHadoopConf(
            String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        Configuration conf = new Configuration();
        if (s3Endpoint != null && !s3Endpoint.isBlank()) {
            conf.set("fs.s3a.endpoint",          s3Endpoint);
            conf.set("fs.s3a.access.key",        s3AccessKey != null ? s3AccessKey : "");
            conf.set("fs.s3a.secret.key",        s3SecretKey != null ? s3SecretKey : "");
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem");

            // MinIO multipart upload tuning
            String multipartSize      = senv("ICEBERG_S3_MULTIPART_SIZE",      "67108864");
            String multipartThreshold = senv("ICEBERG_S3_MULTIPART_THRESHOLD",  "67108864");
            conf.set("fs.s3a.multipart.size",               multipartSize);
            conf.set("fs.s3a.multipart.threshold",          multipartThreshold);
            // Parallel upload threads per file; increase for high-bandwidth links
            conf.set("fs.s3a.threads.max",
                    senv("ICEBERG_S3_UPLOAD_THREADS", "10"));
        }
        return conf;
    }

    /**
     * Iceberg FlinkSink write properties sourced from environment variables.
     * These map directly to {@code write.*} Iceberg table properties.
     */
    private static Map<String, String> buildWriteProperties() {
        Map<String, String> props = new HashMap<>();

        // Target data file size before the writer rolls to a new file.
        // Smaller → more S3 objects (higher PUT cost, faster partial reads).
        // Larger  → fewer objects, but compaction must merge larger files.
        props.put("write.target-file-size-bytes",
                senv("ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES", "134217728"));

        // File format: parquet is the default (columnar, good for analytics).
        // avro is row-oriented (faster writes, less efficient scans).
        // orc is columnar with better predicate pushdown than parquet for some engines.
        props.put("write.format.default",
                senv("ICEBERG_WRITE_FORMAT", "parquet"));

        // Parquet row-group size; affects read buffer and compression ratio.
        props.put("write.parquet.row-group-size-bytes",
                senv("ICEBERG_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES", "134217728"));

        // Parquet page size; smaller pages improve predicate pushdown recall.
        props.put("write.parquet.page-size-bytes",
                senv("ICEBERG_WRITE_PARQUET_PAGE_SIZE_BYTES", "1048576"));

        // Compaction: target file size for RewriteDataFilesAction (minor compaction).
        // Should generally match the write target to avoid re-compacting repeatedly.
        props.put("write.target-file-size-bytes",
                senv("ICEBERG_COMPACTION_TARGET_FILE_SIZE",
                     senv("ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES", "134217728")));

        return props;
    }

    /** Write parallelism for the Iceberg sink operators; -1 defers to Flink's env default. */
    private static int writeParallelism() {
        String v = System.getenv("ICEBERG_WRITE_PARALLELISM");
        if (v != null && !v.isBlank()) {
            return Integer.parseInt(v);
        }
        return -1;
    }

    private static String senv(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
