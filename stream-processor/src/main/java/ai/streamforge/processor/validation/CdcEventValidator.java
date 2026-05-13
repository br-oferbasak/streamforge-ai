package ai.streamforge.processor.validation;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.QuarantineEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Set;

/**
 * Inline schema, null, and range validator for {@link CdcEvent} records.
 *
 * <p>Valid events pass through to the main output unchanged. Events that fail
 * any check are routed to {@link #QUARANTINE_TAG} as a {@link QuarantineEvent}
 * (preserving the raw JSON for reprocessing) and are not forwarded downstream.
 *
 * <h3>Rules applied (in order)</h3>
 * <ol>
 *   <li><b>Schema</b> — {@code op} must be one of {@code c, u, d, r}.</li>
 *   <li><b>Range — timestamp</b> — {@code tsMs} must be &gt; {@value #TS_FLOOR_MS}
 *       (2020-01-01) and not more than {@code maxFutureSkewMs} ahead of wall-clock.</li>
 *   <li><b>Null</b> — for insert ({@code op=c}) and snapshot ({@code op=r}) rows:
 *       {@code after}, {@code after.userId}, and {@code after.eventType} must be
 *       non-null and non-blank.</li>
 *   <li><b>Range — userId length</b> — between 1 and {@code userIdMaxLen} characters.</li>
 * </ol>
 *
 * <h3>Configuration (env vars read by the job)</h3>
 * <ul>
 *   <li>{@code VALIDATION_MAX_FUTURE_SKEW_MS} — default {@code 3600000} (1 h)</li>
 *   <li>{@code VALIDATION_USER_ID_MAX_LEN}    — default {@code 128}</li>
 * </ul>
 *
 * <h3>Metrics emitted</h3>
 * <ul>
 *   <li>{@code validation_passed_total}</li>
 *   <li>{@code validation_quarantined_total}</li>
 *   <li>{@code validation_quarantine_null_field_total}</li>
 *   <li>{@code validation_quarantine_range_violation_total}</li>
 *   <li>{@code validation_quarantine_schema_mismatch_total}</li>
 * </ul>
 */
public class CdcEventValidator extends ProcessFunction<CdcEvent, CdcEvent> {

    public static final OutputTag<QuarantineEvent> QUARANTINE_TAG =
            new OutputTag<QuarantineEvent>("quarantine") {};

    // Events before 2020-01-01 are clock errors or test data.
    static final long TS_FLOOR_MS = 1_577_836_800_000L;

    static final Set<String> VALID_OPS   = Set.of("c", "u", "d", "r");
    static final int         USER_ID_MAX = 128;

    private final long maxFutureSkewMs;
    private final int  userIdMaxLen;

    private transient ObjectMapper objectMapper;
    private transient Counter      passedCounter;
    private transient Counter      quarantinedCounter;
    private transient Counter      nullFieldCounter;
    private transient Counter      rangeViolationCounter;
    private transient Counter      schemaMismatchCounter;

    public CdcEventValidator() {
        this(3_600_000L, USER_ID_MAX);
    }

    public CdcEventValidator(long maxFutureSkewMs, int userIdMaxLen) {
        this.maxFutureSkewMs = maxFutureSkewMs;
        this.userIdMaxLen    = userIdMaxLen;
    }

    @Override
    public void open(Configuration parameters) {
        objectMapper          = new ObjectMapper();
        passedCounter         = counter("validation_passed_total");
        quarantinedCounter    = counter("validation_quarantined_total");
        nullFieldCounter      = counter("validation_quarantine_null_field_total");
        rangeViolationCounter = counter("validation_quarantine_range_violation_total");
        schemaMismatchCounter = counter("validation_quarantine_schema_mismatch_total");
    }

    @Override
    public void processElement(CdcEvent event, Context ctx, Collector<CdcEvent> out) {
        String violation = validate(event, System.currentTimeMillis());
        if (violation == null) {
            passedCounter.inc();
            out.collect(event);
        } else {
            quarantinedCounter.inc();
            ctx.output(QUARANTINE_TAG,
                    new QuarantineEvent(event, toRawJson(event),
                            violationType(violation), violation));
        }
    }

    // ── Validation logic (package-visible for unit testing) ──────────────────

    /**
     * Returns {@code null} if the event passes all checks, or a
     * {@code "VIOLATION_TYPE: detail"} string on failure.
     *
     * @param nowMs current wall-clock time in ms (injectable for deterministic tests)
     */
    String validate(CdcEvent event, long nowMs) {
        // 1. Schema: op-code
        if (event.op == null || !VALID_OPS.contains(event.op)) {
            schemaMismatchCounter.inc();
            return ViolationType.SCHEMA_MISMATCH.label()
                    + ": op='" + event.op + "' not in " + VALID_OPS;
        }

        // 2. Range: timestamp floor
        if (event.tsMs < TS_FLOOR_MS) {
            rangeViolationCounter.inc();
            return ViolationType.RANGE_VIOLATION.label()
                    + ": tsMs=" + event.tsMs + " is before floor "
                    + TS_FLOOR_MS + " (2020-01-01)";
        }

        // 3. Range: timestamp ceiling (clock-skew guard)
        if (event.tsMs > nowMs + maxFutureSkewMs) {
            rangeViolationCounter.inc();
            return ViolationType.RANGE_VIOLATION.label()
                    + ": tsMs=" + event.tsMs + " is "
                    + (event.tsMs - nowMs) + "ms in the future"
                    + " (max skew=" + maxFutureSkewMs + "ms)";
        }

        // 4. Null + range checks for rows that carry payload
        if ("c".equals(event.op) || "r".equals(event.op)) {
            if (event.after == null) {
                nullFieldCounter.inc();
                return ViolationType.NULL_FIELD.label()
                        + ": after is null for op=" + event.op;
            }
            if (event.after.userId == null || event.after.userId.isBlank()) {
                nullFieldCounter.inc();
                return ViolationType.NULL_FIELD.label()
                        + ": after.userId is null or blank";
            }
            if (event.after.userId.length() > userIdMaxLen) {
                rangeViolationCounter.inc();
                return ViolationType.RANGE_VIOLATION.label()
                        + ": after.userId length " + event.after.userId.length()
                        + " exceeds max " + userIdMaxLen;
            }
            if (event.after.eventType == null || event.after.eventType.isBlank()) {
                nullFieldCounter.inc();
                return ViolationType.NULL_FIELD.label()
                        + ": after.eventType is null or blank";
            }
        }

        return null;
    }

    String toRawJson(CdcEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            return "{\"error\":\"serialization failed: " + e.getMessage() + "\"}";
        }
    }

    private static String violationType(String violationMsg) {
        int colon = violationMsg.indexOf(':');
        return colon > 0 ? violationMsg.substring(0, colon) : violationMsg;
    }

    private Counter counter(String name) {
        return getRuntimeContext().getMetricGroup().counter(name);
    }
}
