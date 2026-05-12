package ai.streamforge.processor.sink;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds and attaches the primary Apache Iceberg sink to a {@link UserEventCount} stream.
 *
 * <h2>Schema</h2>
 * <pre>
 *   user_id         STRING  NOT NULL
 *   event_count     LONG    NOT NULL
 *   window_start_ms LONG    NOT NULL
 *   window_end_ms   LONG    NOT NULL
 *   event_date      STRING  NOT NULL   -- YYYY-MM-DD, derived from window_start_ms (UTC)
 * </pre>
 *
 * <h2>Partitioning</h2>
 * The table is partitioned by {@code identity(event_date)}, which places all records
 * for a given calendar day (UTC) in the same S3 directory. This gives:
 * <ul>
 *   <li>Efficient time-range queries — planners prune entire day directories.</li>
 *   <li>Predictable compaction scope — each day's files form an independent group.</li>
 *   <li>Safe late-data handling — late events land in the correct day partition
 *       without touching already-compacted older partitions.</li>
 * </ul>
 *
 * <h2>Catalog types</h2>
 * {@code hadoop} (default), {@code hive}, {@code rest}.
 * For MinIO / S3-compatible storage set the {@code ICEBERG_S3_*} environment variables.
 *
 * <h2>Tuning knobs (environment variables)</h2>
 * <ul>
 *   <li>{@code ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES} — target data-file size before rolling,
 *       default {@code 134217728} (128 MB).</li>
 *   <li>{@code ICEBERG_WRITE_FORMAT}                 — {@code parquet} (default), {@code avro},
 *       or {@code orc}.</li>
 *   <li>{@code ICEBERG_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES} — Parquet row-group size,
 *       default {@code 134217728} (128 MB).</li>
 *   <li>{@code ICEBERG_WRITE_PARQUET_PAGE_SIZE_BYTES} — Parquet page size,
 *       default {@code 1048576} (1 MB).</li>
 *   <li>{@code ICEBERG_WRITE_PARALLELISM}            — sink operator parallelism,
 *       default {@code -1} (inherit from env).</li>
 *   <li>{@code ICEBERG_S3_MULTIPART_SIZE}            — S3A multipart part size,
 *       default {@code 67108864} (64 MB).</li>
 *   <li>{@code ICEBERG_S3_MULTIPART_THRESHOLD}       — multipart upload threshold,
 *       default {@code 67108864} (64 MB).</li>
 *   <li>{@code ICEBERG_S3_UPLOAD_THREADS}            — parallel S3A upload threads,
 *       default {@code 10}.</li>
 * </ul>
 */
public class IcebergSinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    /**
     * Table schema. Field IDs are stable — adding new optional fields at the end
     * is backward compatible with existing data files.
     */
    public static final Schema TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "user_id",         Types.StringType.get()),
            Types.NestedField.required(2, "event_count",     Types.LongType.get()),
            Types.NestedField.required(3, "window_start_ms", Types.LongType.get()),
            Types.NestedField.required(4, "window_end_ms",   Types.LongType.get()),
            Types.NestedField.required(5, "event_date",      Types.StringType.get())
    );

    /**
     * Partition by calendar day (UTC) derived from {@code window_start_ms}.
     * Using identity transform keeps partition pruning exact with no transform overhead.
     */
    public static final PartitionSpec PARTITION_SPEC =
            PartitionSpec.builderFor(TABLE_SCHEMA)
                    .identity("event_date")
                    .build();

    // ── Public entry point ───────────────────────────────────────────────────

    /**
     * Maps {@code stream} to Iceberg {@link RowData}, ensures the partitioned table
     * exists, and attaches a {@link FlinkSink} in append mode.
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
                .map((MapFunction<UserEventCount, RowData>) IcebergSinkFactory::toRowData)
                .returns(TypeInformation.of(RowData.class))
                .name("Map to Iceberg RowData");

        Map<String, String> writeProps = buildWriteProperties();
        int writePar = writeParallelism();

        FlinkSink.Builder sinkBuilder = FlinkSink.forRowData(rowData)
                .tableLoader(tableLoader)
                .setAll(writeProps)
                .append();

        if (writePar > 0) {
            sinkBuilder.writeParallelism(writePar);
        }

        sinkBuilder.name("Iceberg Sink: " + database + "." + tableName);

        LOG.info("Iceberg sink attached: catalog={}, warehouse={}, table={}.{}, " +
                 "partition=identity(event_date), writeProps={}",
                catalogType, warehouse, database, tableName, writeProps);
    }

    // ── RowData mapping ──────────────────────────────────────────────────────

    /** Converts a {@link UserEventCount} to the 5-field Iceberg {@link RowData}. */
    static RowData toRowData(UserEventCount e) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, StringData.fromString(e.userId));
        row.setField(1, e.count);
        row.setField(2, e.windowStartMs);
        row.setField(3, e.windowEndMs);
        row.setField(4, StringData.fromString(toEventDate(e.windowStartMs)));
        return row;
    }

    /** Returns a {@code YYYY-MM-DD} string (UTC) for the given epoch-millisecond timestamp. */
    static String toEventDate(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .toString();
    }

    // ── Catalog / table helpers ──────────────────────────────────────────────

    static CatalogLoader buildCatalogLoader(
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

    static void ensureTable(
            CatalogLoader catalogLoader, TableIdentifier tableId, String database) {
        Catalog catalog = catalogLoader.loadCatalog();
        Namespace ns = Namespace.of(database);
        if (!catalog.namespaceExists(ns)) {
            catalog.createNamespace(ns);
        }
        if (!catalog.tableExists(tableId)) {
            Table table = catalog.createTable(tableId, TABLE_SCHEMA, PARTITION_SPEC);
            // Set default write properties on the newly created table
            table.updateProperties()
                    .set("write.target-file-size-bytes",
                            senv("ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES", "134217728"))
                    .set("write.format.default",
                            senv("ICEBERG_WRITE_FORMAT", "parquet"))
                    .commit();
            LOG.info("Created partitioned Iceberg table {} with partition spec {}",
                    tableId, PARTITION_SPEC);
        }
    }

    // ── Write properties ─────────────────────────────────────────────────────

    static Map<String, String> buildWriteProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("write.target-file-size-bytes",
                senv("ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES", "134217728"));
        props.put("write.format.default",
                senv("ICEBERG_WRITE_FORMAT", "parquet"));
        props.put("write.parquet.row-group-size-bytes",
                senv("ICEBERG_WRITE_PARQUET_ROW_GROUP_SIZE_BYTES", "134217728"));
        props.put("write.parquet.page-size-bytes",
                senv("ICEBERG_WRITE_PARQUET_PAGE_SIZE_BYTES", "1048576"));
        return props;
    }

    static int writeParallelism() {
        String v = System.getenv("ICEBERG_WRITE_PARALLELISM");
        return (v != null && !v.isBlank()) ? Integer.parseInt(v) : -1;
    }

    // ── Hadoop / S3A config ──────────────────────────────────────────────────

    static Configuration buildHadoopConf(
            String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        Configuration conf = new Configuration();
        if (s3Endpoint != null && !s3Endpoint.isBlank()) {
            conf.set("fs.s3a.endpoint",          s3Endpoint);
            conf.set("fs.s3a.access.key",        s3AccessKey != null ? s3AccessKey : "");
            conf.set("fs.s3a.secret.key",        s3SecretKey != null ? s3SecretKey : "");
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem");
            conf.set("fs.s3a.multipart.size",
                    senv("ICEBERG_S3_MULTIPART_SIZE", "67108864"));
            conf.set("fs.s3a.multipart.threshold",
                    senv("ICEBERG_S3_MULTIPART_THRESHOLD", "67108864"));
            conf.set("fs.s3a.threads.max",
                    senv("ICEBERG_S3_UPLOAD_THREADS", "10"));
        }
        return conf;
    }

    private static String senv(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
