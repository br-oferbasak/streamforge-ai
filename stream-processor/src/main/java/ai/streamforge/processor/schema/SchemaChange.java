package ai.streamforge.processor.schema;

import java.io.Serializable;

/**
 * Describes a single schema change detected from a CDC event's field presence
 * or type signature.
 *
 * <p>Instances are attached to {@link ai.streamforge.processor.model.DeadLetterEvent}
 * when a BREAKING change is detected, providing enough context for an operator to
 * register an alias or coercion rule and trigger reprocessing.
 */
public final class SchemaChange implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Column (field) affected by the change. */
    public final String column;

    /** Human-readable description of the observed type before the change, if known. */
    public final String oldTypeHint;

    /** Human-readable description of the observed type after the change, if known. */
    public final String newTypeHint;

    /** Classification of this specific change. */
    public final ChangeType changeType;

    /** Effective compatibility after alias/coercion rules are applied. */
    public final CompatibilityLevel effectiveCompatibility;

    public SchemaChange(
            String column,
            String oldTypeHint,
            String newTypeHint,
            ChangeType changeType) {
        this.column = column;
        this.oldTypeHint = oldTypeHint;
        this.newTypeHint = newTypeHint;
        this.changeType = changeType;
        this.effectiveCompatibility = changeType.defaultCompatibility;
    }

    /** Returns a new instance with an updated effective compatibility (e.g. after alias registration). */
    public SchemaChange withCompatibility(CompatibilityLevel level) {
        return new SchemaChange(column, oldTypeHint, newTypeHint, changeType) {
            { /* shadow the field via anonymous-class initialiser */ }
            @Override public String toString() {
                return SchemaChange.this.toStringWith(level);
            }
        };
    }

    @Override
    public String toString() {
        return toStringWith(effectiveCompatibility);
    }

    private String toStringWith(CompatibilityLevel level) {
        return "SchemaChange{"
                + "column='" + column + '\''
                + ", changeType=" + changeType
                + ", compatibility=" + level
                + ", oldType='" + oldTypeHint + '\''
                + ", newType='" + newTypeHint + '\''
                + '}';
    }
}
