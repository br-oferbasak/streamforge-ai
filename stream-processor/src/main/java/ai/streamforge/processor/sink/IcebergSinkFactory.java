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
 * <h2>Supported catalog types</h2>
 * <dl>
 *   <dt>{@code hadoop} (default)</dt>
 *   <dd>File-system catalog backed by a Hadoop-compatible path.  S3A is used for
 *       MinIO/S3; configure via the {@code ICEBERG_S3_*} env vars.</dd>
 *   <dt>{@code hive}</dt>
 *   <dd>Hive Metastore.  Set {@code ICEBERG_WAREHOUSE} to the metastore URI
 *       (e.g. {@code thrift://hive-metastore:9083}).</dd>
 *   <dt>{@code rest}</dt>
 *   <dd>Iceberg REST Catalog (spec v1).  Set {@code ICEBERG_REST_URI} to the
 *       server base URL (e.g. {@code http://iceberg-rest:8181}).  The Flink job
 *       writes data files directly to S3/MinIO via Iceberg's native
 *       {@code S3FileIO}; set the {@code ICEBERG_S3_*} vars so the job can reach
 *       the object store.</dd>
 * </dl>
 *
 * <h2>Discoverability</h2>
 * When {@code rest} is used the table is registered in a shared catalog that any
 * Iceberg-compatible query engine (Trino, Spark, PyIceberg, etc.) can discover
 * by pointing at the same REST server URL.  Other engines see the exact same
 * namespace ({@code ICEBERG_DATABASE}) and table ({@code ICEBERG_TABLE}).
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
     * Attaches an Iceberg sink to {@code stream}, creating the namespace and table
     * if they do not already exist.
     *
     * @param restUri   REST catalog server base URL; required when
     *                  {@code catalogType=rest}, ignored otherwise.
     */
    public static void attach(
            DataStream<UserEventCount> stream,
            String catalogType,
            String warehouse,
            String database,
            String tableName,
            String s3Endpoint,
            String s3AccessKey,
            String s3SecretKey,
            String restUri) {

        Map<String, String> props = buildCatalogProps(
                catalogType, warehouse, s3Endpoint, s3AccessKey, s3SecretKey, restUri);
        Configuration hadoopConf  = buildHadoopConf(catalogType, s3Endpoint, s3AccessKey, s3SecretKey);
        CatalogLoader catalogLoader = buildCatalogLoader(catalogType, hadoopConf, props);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        ensureTable(catalogLoader, tableId, database);

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        DataStream<RowData> rowData = stream
                .map((MapFunction<UserEventCount, RowData>) IcebergSinkFactory::toRowData)
                .returns(TypeInformation.of(RowData.class))
                .name("Map to Iceberg RowData");

        Map<String, String> writeProps = buildWriteProperties();

        FlinkSink.forRowData(rowData)
                .tableLoader(tableLoader)
                .writeParallelism(writeParallelism())
                .setAll(writeProps)
                .append()
                .name("Iceberg Sink: " + database + "." + tableName);

        LOG.info("Iceberg sink attached: catalog={}, table={}.{}", catalogType, database, tableName);
    }

    // ── Catalog configuration ────────────────────────────────────────────────

    /**
     * Builds catalog properties appropriate for each catalog type.
     *
     * <p>For {@code rest}: uses Iceberg's native {@code S3FileIO} so that data files
     * are written directly to MinIO without going through Hadoop S3A.  The REST
     * server itself never proxies file I/O — the Flink job writes files directly
     * and only reports the resulting manifest/snapshot to the catalog.
     */
    static Map<String, String> buildCatalogProps(
            String catalogType,
            String warehouse,
            String s3Endpoint,
            String s3AccessKey,
            String s3SecretKey,
            String restUri) {

        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);

        if ("rest".equalsIgnoreCase(catalogType)) {
            if (restUri == null || restUri.isBlank()) {
                throw new IllegalArgumentException(
                        "ICEBERG_REST_URI must be set when ICEBERG_CATALOG_TYPE=rest");
            }
            props.put("uri", restUri);

            // S3FileIO: Iceberg-native S3 client; no Hadoop dependency on the write path.
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            if (s3Endpoint != null && !s3Endpoint.isBlank()) {
                props.put("s3.endpoint",           s3Endpoint);
                props.put("s3.access-key-id",      s3AccessKey != null ? s3AccessKey : "");
                props.put("s3.secret-access-key",  s3SecretKey != null ? s3SecretKey : "");
                props.put("s3.path-style-access",  "true");
            }
        }

        return props;
    }

    static CatalogLoader buildCatalogLoader(
            String catalogType, Configuration hadoopConf, Map<String, String> props) {
        return switch (catalogType.toLowerCase()) {
            case "hadoop" -> CatalogLoader.hadoop("streamforge", hadoopConf, props);
            case "hive"   -> CatalogLoader.hive("streamforge", hadoopConf, props);
            case "rest"   -> CatalogLoader.rest("streamforge", hadoopConf, props);
            default -> throw new IllegalArgumentException(
                    "Unsupported ICEBERG_CATALOG_TYPE: '" + catalogType
                    + "'. Valid: hadoop, hive, rest");
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

    /**
     * Hadoop config is only used for {@code hadoop} and {@code hive} catalog types.
     * For {@code rest}, file I/O is handled by {@code S3FileIO} via catalog properties.
     */
    private static Configuration buildHadoopConf(
            String catalogType, String s3Endpoint, String s3AccessKey, String s3SecretKey) {
        Configuration conf = new Configuration();
        if ("rest".equalsIgnoreCase(catalogType)) {
            return conf; // S3FileIO doesn't use Hadoop conf
        }
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
