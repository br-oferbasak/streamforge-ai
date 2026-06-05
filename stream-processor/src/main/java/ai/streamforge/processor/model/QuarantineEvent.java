package ai.streamforge.processor.model;

/** A CDC row that failed inline validation and has been quarantined for review or reprocessing. */
public class QuarantineEvent {

    /** Debezium op-code of the original event, or {@code "unknown"} if unreadable. */
    public String op;

    /** Source event timestamp (ms); 0 if absent. */
    public long tsMs;

    /** Full JSON of the original {@link CdcEvent}, preserved for reprocessing. */
    public String rawPayload;

    /** Category of the validation failure. */
    public String violationType;

    /** Human-readable description of what failed and why. */
    public String violationMessage;

    /** Wall-clock time the event was quarantined (ms). */
    public long quarantinedAtMs;

    public QuarantineEvent() {}

    public QuarantineEvent(
            CdcEvent source,
            String rawPayload,
            String violationType,
            String violationMessage) {
        this.op               = source.op != null ? source.op : "unknown";
        this.tsMs             = source.tsMs;
        this.rawPayload       = rawPayload;
        this.violationType    = violationType;
        this.violationMessage = violationMessage;
        this.quarantinedAtMs  = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "QuarantineEvent{op='" + op + "', violation=" + violationType
                + ", message='" + violationMessage + "'}";
    }
}
