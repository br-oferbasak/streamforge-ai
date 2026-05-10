package ai.streamforge.processor.model;

import ai.streamforge.processor.schema.ChangeType;
import ai.streamforge.processor.schema.CompatibilityLevel;

/**
 * A CDC record that could not be processed; routed to the dead-letter Kafka topic.
 *
 * <p>The {@link #changeType} and {@link #schemaVersion} fields carry enough context
 * for an operator to determine the remediation path:
 * <ul>
 *   <li>{@link ChangeType#DROP_COLUMN} on a required field →
 *       register an alias or fix the source schema; then replay from this offset.</li>
 *   <li>{@link ChangeType#RENAME_COLUMN} →
 *       register an alias via {@code COLUMN_ALIASES} env var; take a savepoint and
 *       redeploy; then use the {@code DlqReprocessingJob} to replay this event.</li>
 *   <li>{@link ChangeType#INCOMPATIBLE_TYPE} →
 *       fix the source schema; full reprocessing from earliest offset may be needed.</li>
 * </ul>
 */
public class DeadLetterEvent {

    /** Raw JSON bytes of the original Debezium envelope. */
    public String rawPayload;

    /** Human-readable description of why the event was rejected. */
    public String errorMessage;

    /** Epoch-ms when this DLQ entry was created. */
    public long failedAtMs;

    /**
     * Type of schema change that caused the rejection, or {@code null} if the failure
     * was a parse error unrelated to schema evolution.
     */
    public ChangeType changeType;

    /**
     * Schema version detected before the event was rejected.
     * Helps correlate DLQ spikes with specific DDL migrations.
     */
    public SchemaVersion schemaVersion;

    /** Column affected by the change, if known. */
    public String affectedColumn;

    /** Effective compatibility level of the detected change. */
    public CompatibilityLevel compatibilityLevel;

    public DeadLetterEvent() {}

    public DeadLetterEvent(String rawPayload, String errorMessage) {
        this.rawPayload   = rawPayload;
        this.errorMessage = errorMessage;
        this.failedAtMs   = System.currentTimeMillis();
    }

    public DeadLetterEvent(
            String rawPayload,
            String errorMessage,
            ChangeType changeType,
            SchemaVersion schemaVersion,
            String affectedColumn,
            CompatibilityLevel compatibilityLevel) {
        this(rawPayload, errorMessage);
        this.changeType         = changeType;
        this.schemaVersion      = schemaVersion;
        this.affectedColumn     = affectedColumn;
        this.compatibilityLevel = compatibilityLevel;
    }

    @Override
    public String toString() {
        return "DeadLetterEvent{"
                + "error='" + errorMessage + '\''
                + ", changeType=" + changeType
                + ", schemaVersion=" + schemaVersion
                + ", affectedColumn='" + affectedColumn + '\''
                + ", compatibility=" + compatibilityLevel
                + ", failedAtMs=" + failedAtMs
                + '}';
    }
}
