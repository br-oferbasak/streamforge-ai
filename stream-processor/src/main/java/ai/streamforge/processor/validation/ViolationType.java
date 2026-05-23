package ai.streamforge.processor.validation;

/** Categories of inline validation failure used in {@link CdcEventValidator}. */
public enum ViolationType {

    /** A required field is null or blank. */
    NULL_FIELD,

    /** A field value falls outside its permitted range. */
    RANGE_VIOLATION,

    /** The op-code is not one of the recognised Debezium values. */
    SCHEMA_MISMATCH;

    public String label() {
        return name();
    }
}
