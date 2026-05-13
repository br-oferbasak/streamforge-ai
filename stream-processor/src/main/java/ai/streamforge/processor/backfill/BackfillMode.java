package ai.streamforge.processor.backfill;

/** Replay strategy for {@link BackfillJob}. */
public enum BackfillMode {

    /**
     * Replay a closed timestamp interval from the CDC topic.
     * Requires {@code BACKFILL_START_TIMESTAMP_MS}; {@code BACKFILL_END_TIMESTAMP_MS}
     * defaults to now.
     */
    KAFKA_RANGE,

    /**
     * Read from the earliest offset to the latest offset at job-start.
     * Accepts both {@code op=c} and {@code op=r} (Debezium snapshot reads).
     */
    CDC_SNAPSHOT;

    public static BackfillMode parse(String value) {
        return switch (value.toLowerCase().replace('-', '_')) {
            case "kafka_range"   -> KAFKA_RANGE;
            case "cdc_snapshot"  -> CDC_SNAPSHOT;
            default -> throw new IllegalArgumentException(
                    "Unknown BACKFILL_MODE: '" + value + "'. Valid values: kafka_range, cdc_snapshot");
        };
    }
}
