package ai.streamforge.processor.schema;

/**
 * Compatibility classification for a Debezium schema change.
 *
 * <pre>
 * BACKWARD  – new schema can read data written with the old schema.
 *             Flink job keeps running; null-fill / alias handles gaps.
 *             Example: ADD COLUMN (nullable), type widening (INT → BIGINT).
 *
 * FORWARD   – old schema can read data written with the new schema
 *             (unknown fields are silently ignored by {@code @JsonIgnoreProperties}).
 *             Example: DROP COLUMN that is optional in processing logic.
 *
 * FULL      – both BACKWARD and FORWARD; the safest possible change.
 *             Example: ADD COLUMN nullable with a registered default.
 *
 * BREAKING  – neither direction is safe without intervention.
 *             Flink job must be stopped; events land in the DLQ.
 *             Reprocessing is required after the alias or coercion rule is registered.
 *             Example: DROP required column, type narrowing, rename without alias.
 * </pre>
 */
public enum CompatibilityLevel {
    BACKWARD,
    FORWARD,
    FULL,
    BREAKING
}
