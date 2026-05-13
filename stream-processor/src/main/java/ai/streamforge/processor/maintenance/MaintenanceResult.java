package ai.streamforge.processor.maintenance;

/** Summary produced by a single run of {@link IcebergMaintenanceJob}. */
public class MaintenanceResult {

    public final String  tableIdentifier;

    // Snapshot expiry
    public final int     snapshotsExpired;
    public final long    dataFilesDeletedByExpiry;
    public final long    manifestFilesDeletedByExpiry;

    // Manifest compaction
    public final int     manifestsBeforeRewrite;
    public final int     manifestsAfterRewrite;

    // Data file health (informational — compaction is a separate Flink batch run)
    public final long    totalDataFiles;
    public final long    smallDataFiles;        // below COMPACT_SMALL_FILE_THRESHOLD_BYTES
    public final long    totalDataSizeBytes;
    public final int     partitionsScanned;

    public final long    durationMs;

    public MaintenanceResult(
            String tableIdentifier,
            int snapshotsExpired,
            long dataFilesDeletedByExpiry,
            long manifestFilesDeletedByExpiry,
            int manifestsBeforeRewrite,
            int manifestsAfterRewrite,
            long totalDataFiles,
            long smallDataFiles,
            long totalDataSizeBytes,
            int partitionsScanned,
            long durationMs) {
        this.tableIdentifier             = tableIdentifier;
        this.snapshotsExpired            = snapshotsExpired;
        this.dataFilesDeletedByExpiry    = dataFilesDeletedByExpiry;
        this.manifestFilesDeletedByExpiry = manifestFilesDeletedByExpiry;
        this.manifestsBeforeRewrite      = manifestsBeforeRewrite;
        this.manifestsAfterRewrite       = manifestsAfterRewrite;
        this.totalDataFiles              = totalDataFiles;
        this.smallDataFiles              = smallDataFiles;
        this.totalDataSizeBytes          = totalDataSizeBytes;
        this.partitionsScanned           = partitionsScanned;
        this.durationMs                  = durationMs;
    }

    @Override
    public String toString() {
        return String.format(
            "MaintenanceResult{table=%s, snapshotsExpired=%d, " +
            "manifestsRewritten=%d→%d, " +
            "dataFiles=%d (small=%d, %.1f%%), totalSize=%.1f MB, " +
            "partitions=%d, duration=%dms}",
            tableIdentifier,
            snapshotsExpired,
            manifestsBeforeRewrite, manifestsAfterRewrite,
            totalDataFiles, smallDataFiles,
            totalDataFiles > 0 ? 100.0 * smallDataFiles / totalDataFiles : 0.0,
            totalDataSizeBytes / 1e6,
            partitionsScanned,
            durationMs);
    }
}
