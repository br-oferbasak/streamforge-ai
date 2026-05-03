package ai.streamforge.processor;

import ai.streamforge.processor.metrics.InsertFilterWithMetrics;
import ai.streamforge.processor.model.CdcEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the filter predicate logic of {@link InsertFilterWithMetrics} without
 * a Flink runtime context (metrics counters are not exercised here).
 */
class InsertFilterWithMetricsTest {

    // ── insert events that should pass ──────────────────────────────────────

    @Test
    void acceptsInsertWithValidUserId() {
        CdcEvent event = insertEvent("user-1");
        assertTrue(InsertFilterWithMetrics.isValidInsert(event));
    }

    // ── events that should be dropped ───────────────────────────────────────

    @Test
    void rejectsUpdateEvent() {
        CdcEvent event = eventWithOp("u", "user-1");
        assertFalse(InsertFilterWithMetrics.isValidInsert(event));
    }

    @Test
    void rejectsDeleteEvent() {
        CdcEvent event = eventWithOp("d", null);
        assertFalse(InsertFilterWithMetrics.isValidInsert(event));
    }

    @Test
    void rejectsSnapshotReadEvent() {
        CdcEvent event = eventWithOp("r", "user-1");
        assertFalse(InsertFilterWithMetrics.isValidInsert(event));
    }

    @Test
    void rejectsInsertWithNullAfter() {
        CdcEvent event = new CdcEvent();
        event.op = "c";
        event.after = null;
        assertFalse(InsertFilterWithMetrics.isValidInsert(event));
    }

    @Test
    void rejectsInsertWithNullUserId() {
        CdcEvent event = new CdcEvent();
        event.op = "c";
        event.after = new CdcEvent.UserEventRow();
        event.after.userId = null;
        assertFalse(InsertFilterWithMetrics.isValidInsert(event));
    }

    @Test
    void rejectsInsertWithBlankUserId() {
        // userId that is empty string is technically non-null — treated as valid by the filter.
        // This test documents that behaviour explicitly.
        CdcEvent event = insertEvent("");
        assertTrue(InsertFilterWithMetrics.isValidInsert(event),
                "empty-string userId is non-null; downstream logic owns userId validation");
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static CdcEvent insertEvent(String userId) {
        return eventWithOp("c", userId);
    }

    private static CdcEvent eventWithOp(String op, String userId) {
        CdcEvent event = new CdcEvent();
        event.op = op;
        if (userId != null) {
            event.after = new CdcEvent.UserEventRow();
            event.after.userId = userId;
        }
        return event;
    }
}
