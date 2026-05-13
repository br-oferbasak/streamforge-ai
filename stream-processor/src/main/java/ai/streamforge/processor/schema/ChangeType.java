package ai.streamforge.processor.schema;

/**
 * Taxonomy of schema changes Debezium can surface via CDC events.
 *
 * Each value carries a pre-computed {@link CompatibilityLevel} that reflects
 * the *default* classification before any alias or coercion rule is applied.
 * After a rule is registered (e.g. a column alias for a rename) the change
 * may be re-classified as BACKWARD.
 */
public enum ChangeType {

    /** New nullable column added to the table. Old events lack the field → null-filled. */
    ADD_COLUMN(CompatibilityLevel.FULL),

    /**
     * Column removed from the table.
     * Compatible if the column is optional in the processing logic (null-safe).
     * Breaking if the column is required (e.g. user_id).
     */
    DROP_COLUMN(CompatibilityLevel.FORWARD),

    /**
     * Column renamed without a registered alias.
     * Events written under the new name will miss the old canonical field name.
     * After the alias is registered this degrades to ADD_COLUMN + DROP_COLUMN.
     */
    RENAME_COLUMN(CompatibilityLevel.BREAKING),

    /**
     * Numeric type widened (e.g. INT → BIGINT, FLOAT → DOUBLE).
     * Old values always fit in the wider type; Flink reads them as longs/doubles.
     */
    WIDEN_TYPE(CompatibilityLevel.BACKWARD),

    /**
     * Numeric type narrowed (e.g. BIGINT → INT).
     * Values that exceed the narrower range will overflow or truncate.
     */
    NARROW_TYPE(CompatibilityLevel.BREAKING),

    /**
     * Column type changed in a structurally incompatible way (e.g. INT → VARCHAR).
     * JSON representation changes; downstream parsing will fail.
     */
    INCOMPATIBLE_TYPE(CompatibilityLevel.BREAKING),

    /** Change could not be classified — treat conservatively as BREAKING. */
    UNKNOWN(CompatibilityLevel.BREAKING);

    public final CompatibilityLevel defaultCompatibility;

    ChangeType(CompatibilityLevel level) {
        this.defaultCompatibility = level;
    }
}
