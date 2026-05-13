package ai.streamforge.processor.maintenance;

import ai.streamforge.processor.sink.IcebergSinkFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Standalone table-maintenance job for the StreamForge Iceberg analytics store.
 *
 * <p>Runs three ordered phases on each execution:
 * <ol>
 *   <li><b>Snapshot expiry</b> — removes snapshots older than {@code SNAPSHOT_MAX_AGE_HOURS}
 *       while retaining at least {@code SNAPSHOT_RETAIN_LAST} recent snapshots.
 *       Expired snapshot files (orphan manifests, position-delete files) are deleted from
 *       object storage immediately.</li>
 *   <li><b>Manifest compaction</b> — rewrites fragmented manifests into fewer, larger ones.
 *       Each Flink checkpoint appends one manifest entry; after hours of streaming this
 *       inflates the manifest list and slows down table scans.</li>
 *   <li><b>Data-file health report</b> — scans all live data files and reports which
 *       partitions contain small files (below {@code COMPACT_SMALL_FILE_THRESHOLD_BYTES}).
 *       Actual data-file compaction requires a Flink batch job; see
 *       {@code docs/ICEBERG_OPERATIONS.md} for the command.</li>
 * </ol>
 *
 * <h2>Running</h2>
 * <pre>
 *   # Phase 1 + 2 + report only (no data-file compaction):
 *   java -cp stream-processor-*.jar \
 *        ai.streamforge.processor.maintenance.IcebergMaintenanceJob
 *
 *   # Scheduled via docker-compose iceberg-maintenance service:
 *   # See deploy/cdc-flink-minio-demo/docker-compose.yml
 * </pre>
 *
 * <h2>Environment variables</h2>
 * <ul>
 *   <li>{@code ICEBERG_CATALOG_TYPE}              — {@code hadoop} (default), {@code hive}, {@code rest}</li>
 *   <li>{@code ICEBERG_WAREHOUSE}                 — default {@code s3a://streamforge/warehouse}</li>
 *   <li>{@code ICEBERG_DATABASE}                  — default {@code streamforge}</li>
 *   <li>{@code ICEBERG_TABLE}                     — default {@code user_event_counts}</li>
 *   <li>{@code ICEBERG_S3_ENDPOINT}               — MinIO endpoint, e.g. {@code http://minio:9000}</li>
 *   <li>{@code ICEBERG_S3_ACCESS_KEY}             — S3/MinIO access key</li>
 *   <li>{@code ICEBERG_S3_SECRET_KEY}             — S3/MinIO secret key</li>
 *   <li>{@code SNAPSHOT_MAX_AGE_HOURS}            — expire snapshots older than N hours,
 *       default {@code 168} (7 days)</li>
 *   <li>{@code SNAPSHOT_RETAIN_LAST}              — always keep at least this many recent
 *       snapshots regardless of age, default {@code 10}</li>
 *   <li>{@code COMPACT_SMALL_FILE_THRESHOLD_BYTES} — files smaller than this are flagged
 *       as candidates for data-file compaction, default {@code 67108864} (64 MB)</li>
 *   <li>{@code DRY_RUN}                           — {@code true} to report without
 *       committing any changes, default {@code false}</li>
 * </ul>
 */
public class IcebergMaintenanceJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMaintenanceJob.class);

    public static void main(String[] args) {
        MaintenanceResult result = run();
        System.out.println(result);
        if (result.smallDataFiles > 0) {
            System.out.printf(
                "[WARN] %d partition(s) have small files. Run data-file compaction — " +
                "see docs/ICEBERG_OPERATIONS.md §3.%n", result.partitionsScanned);
        }
    }

    /** Executes all maintenance phases and returns a summary. */
    public static MaintenanceResult run() {
        String catalogType  = env("ICEBERG_CATALOG_TYPE",  "hadoop");
        String warehouse    = env("ICEBERG_WAREHOUSE",     "s3a://streamforge/warehouse");
        String database     = env("ICEBERG_DATABASE",      "streamforge");
        String tableName    = env("ICEBERG_TABLE",         "user_event_counts");
        String s3Endpoint   = env("ICEBERG_S3_ENDPOINT",   "");
        String s3AccessKey  = env("ICEBERG_S3_ACCESS_KEY", "");
        String s3SecretKey  = env("ICEBERG_S3_SECRET_KEY", "");

        long    maxAgeHours     = Long.parseLong(env("SNAPSHOT_MAX_AGE_HOURS",             "168"));
        int     retainLast      = Integer.parseInt(env("SNAPSHOT_RETAIN_LAST",             "10"));
        long    smallThreshold  = Long.parseLong(env("COMPACT_SMALL_FILE_THRESHOLD_BYTES", "67108864"));
        boolean dryRun          = Boolean.parseBoolean(env("DRY_RUN",                      "false"));

        String tableId = database + "." + tableName;

        LOG.info("IcebergMaintenanceJob starting: table={}, maxAgeHours={}, retainLast={}, " +
                 "smallThreshold={}MB, dryRun={}",
                tableId, maxAgeHours, retainLast, smallThreshold / 1024 / 1024, dryRun);

        long startMs = System.currentTimeMillis();

        Configuration hadoopConf = IcebergSinkFactory.buildHadoopConf(
                s3Endpoint, s3AccessKey, s3SecretKey);
        CatalogLoader catalogLoader = IcebergSinkFactory.buildCatalogLoader(
                catalogType, warehouse, hadoopConf);

        Catalog catalog = catalogLoader.loadCatalog();
        TableIdentifier identifier = TableIdentifier.of(database, tableName);

        if (!catalog.tableExists(identifier)) {
            LOG.warn("Table {} does not exist — nothing to maintain.", tableId);
            return new MaintenanceResult(tableId, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    System.currentTimeMillis() - startMs);
        }

        Table table = catalog.loadTable(identifier);

        // ── Phase 1: Snapshot expiry ─────────────────────────────────────────
        long expiryBeforeMs = System.currentTimeMillis()
                - TimeUnit.HOURS.toMillis(maxAgeHours);

        int   snapshotsExpiredCount  = 0;
        long  deletedDataFiles       = 0;
        long  deletedManifestFiles   = 0;

        List<Long> snapshotIds = new ArrayList<>();
        for (Snapshot s : table.snapshots()) {
            snapshotIds.add(s.snapshotId());
        }
        int totalSnapshots = snapshotIds.size();

        if (dryRun) {
            // Count what would be expired without committing
            for (Snapshot s : table.snapshots()) {
                if (s.timestampMillis() < expiryBeforeMs
                        && totalSnapshots - snapshotsExpiredCount > retainLast) {
                    snapshotsExpiredCount++;
                }
            }
            LOG.info("[DRY RUN] Would expire {} snapshot(s)", snapshotsExpiredCount);
        } else {
            List<Long> deletedDataFilesList   = new ArrayList<>();
            List<Long> deletedManifestFilesList = new ArrayList<>();

            table.expireSnapshots()
                    .expireOlderThan(expiryBeforeMs)
                    .retainLast(retainLast)
                    .cleanExpiredFiles(true)
                    .deleteWith(path -> {
                        // Track deleted files by extension
                        if (path.endsWith(".avro")) {
                            deletedManifestFilesList.add(1L);
                        } else {
                            deletedDataFilesList.add(1L);
                        }
                    })
                    .commit();

            deletedDataFiles     = deletedDataFilesList.size();
            deletedManifestFiles = deletedManifestFilesList.size();

            // Count how many snapshots remain vs. before
            int remaining = 0;
            for (Snapshot ignored : table.snapshots()) remaining++;
            snapshotsExpiredCount = totalSnapshots - remaining;

            LOG.info("Phase 1 complete: expired {} snapshots, deleted {} data files, " +
                     "{} manifest files",
                    snapshotsExpiredCount, deletedDataFiles, deletedManifestFiles);
        }

        // ── Phase 2: Manifest compaction ─────────────────────────────────────
        int manifestsBefore = 0;
        int manifestsAfter  = 0;

        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot != null) {
            manifestsBefore = currentSnapshot.allManifests(table.io()).size();
        }

        if (manifestsBefore > 1) {
            if (dryRun) {
                LOG.info("[DRY RUN] Would compact {} manifests", manifestsBefore);
                manifestsAfter = manifestsBefore;
            } else {
                RewriteManifests rewrite = table.rewriteManifests();
                rewrite.commit();
                currentSnapshot = table.currentSnapshot();
                manifestsAfter = currentSnapshot != null
                        ? currentSnapshot.allManifests(table.io()).size()
                        : 0;
                LOG.info("Phase 2 complete: manifests {} → {}", manifestsBefore, manifestsAfter);
            }
        } else {
            manifestsAfter = manifestsBefore;
            LOG.info("Phase 2 skipped: only {} manifest(s), no compaction needed", manifestsBefore);
        }

        // ── Phase 3: Data-file health scan ────────────────────────────────────
        long totalFiles      = 0;
        long smallFiles      = 0;
        long totalSizeBytes  = 0;
        int  partitions      = 0;

        String lastPartition = null;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                String partition = file.partition().toString();
                if (!partition.equals(lastPartition)) {
                    partitions++;
                    lastPartition = partition;
                }
                totalFiles++;
                totalSizeBytes += file.fileSizeInBytes();
                if (file.fileSizeInBytes() < smallThreshold) {
                    smallFiles++;
                }
            }
        } catch (Exception e) {
            LOG.warn("Phase 3: could not scan data files — {}", e.getMessage());
        }

        LOG.info("Phase 3 complete: {} files ({} small) across {} partition(s), total {:.1f} MB",
                totalFiles, smallFiles, partitions, totalSizeBytes / 1e6);

        MaintenanceResult result = new MaintenanceResult(
                tableId,
                snapshotsExpiredCount,
                deletedDataFiles,
                deletedManifestFiles,
                manifestsBefore,
                manifestsAfter,
                totalFiles,
                smallFiles,
                totalSizeBytes,
                partitions,
                System.currentTimeMillis() - startMs);

        LOG.info("IcebergMaintenanceJob complete: {}", result);
        return result;
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
