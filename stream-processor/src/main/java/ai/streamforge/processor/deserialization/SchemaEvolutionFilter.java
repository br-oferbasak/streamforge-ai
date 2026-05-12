package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.schema.ChangeType;
import ai.streamforge.processor.schema.CompatibilityLevel;
import ai.streamforge.processor.schema.SchemaChange;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * Routes {@link CdcEvent} records between the main stream and the dead-letter
 * side output based on schema evolution compatibility.
 *
 * <h3>Routing rules (applied in order)</h3>
 * <ol>
 *   <li><b>Null event</b> — silently dropped (parse failure logged by the deserializer).</li>
 *   <li><b>BREAKING schema change detected</b> — any event whose {@code detectedChanges}
 *       list contains a change with {@link CompatibilityLevel#BREAKING} compatibility is
 *       sent to the DLQ.  The {@link DeadLetterEvent} carries the {@link ChangeType},
 *       affected column, and schema version to guide remediation.</li>
 *   <li><b>Insert/snapshot with missing required field</b> — an insert ({@code op=c}) or
 *       snapshot read ({@code op=r}) whose {@code after.userId} could not be resolved is
 *       sent to the DLQ.</li>
 *   <li><b>Non-breaking changes</b> — BACKWARD/FULL changes (type widenings, new nullable
 *       columns) pass through with the {@code detectedChanges} list intact for observability.</li>
 *   <li><b>Everything else</b> — passes through unchanged.</li>
 * </ol>
 *
 * <h3>Wire-up example</h3>
 * <pre>{@code
 * SingleOutputStreamOperator<CdcEvent> filtered =
 *         rawEvents.process(new SchemaEvolutionFilter()).name("Schema Evolution Filter");
 * DataStream<DeadLetterEvent> dlq = filtered.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);
 * }</pre>
 */
public class SchemaEvolutionFilter extends ProcessFunction<CdcEvent, CdcEvent> {

    public static final OutputTag<DeadLetterEvent> DLQ_TAG =
            new OutputTag<DeadLetterEvent>("dead-letter") {};

    @Override
    public void processElement(CdcEvent event, Context ctx, Collector<CdcEvent> out) {
        if (event == null) {
            return;
        }

        DeadLetterEvent dlqEntry = classify(event);
        if (dlqEntry != null) {
            ctx.output(DLQ_TAG, dlqEntry);
            return;
        }

        out.collect(event);
    }

    // ── Routing logic (package-private for unit testing) ─────────────────────

    /**
     * Classifies {@code event} and returns a {@link DeadLetterEvent} if it must be
     * routed to the DLQ, or {@code null} if it should pass through.
     */
    static DeadLetterEvent classify(CdcEvent event) {
        // Check for BREAKING schema changes detected during deserialization
        List<SchemaChange> breaking = breakingChanges(event.detectedChanges);
        if (!breaking.isEmpty()) {
            SchemaChange first = breaking.get(0);
            return new DeadLetterEvent(
                    event.toString(),
                    buildMessage(first),
                    first.changeType,
                    event.schemaVersion,
                    first.column,
                    first.effectiveCompatibility);
        }

        // Inserts/reads missing the required user_id field
        if (isInsertOrRead(event) && lacksUserId(event)) {
            return new DeadLetterEvent(
                    event.toString(),
                    "user_id unresolvable after schema normalization (version=" + event.schemaVersion + ")",
                    ChangeType.DROP_COLUMN,
                    event.schemaVersion,
                    "user_id",
                    CompatibilityLevel.BREAKING);
        }

        return null;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static List<SchemaChange> breakingChanges(List<SchemaChange> changes) {
        if (changes == null) return List.of();
        return changes.stream()
                .filter(c -> c.effectiveCompatibility == CompatibilityLevel.BREAKING)
                .toList();
    }

    private static String buildMessage(SchemaChange change) {
        return String.format(
                "BREAKING schema change detected: %s on column '%s' (%s → %s). "
                + "Register an alias or fix the source schema, then replay from the DLQ.",
                change.changeType, change.column, change.oldTypeHint, change.newTypeHint);
    }

    private static boolean isInsertOrRead(CdcEvent event) {
        return "c".equals(event.op) || "r".equals(event.op);
    }

    private static boolean lacksUserId(CdcEvent event) {
        return event.after == null || event.after.userId == null;
    }
}
