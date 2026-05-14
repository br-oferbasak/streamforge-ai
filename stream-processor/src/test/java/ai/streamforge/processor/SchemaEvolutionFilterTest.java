package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.SchemaVersion;
import ai.streamforge.processor.schema.ChangeType;
import ai.streamforge.processor.schema.CompatibilityLevel;
import ai.streamforge.processor.schema.SchemaChange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests {@link SchemaEvolutionFilter#classify(CdcEvent)} routing logic without
 * a Flink runtime harness, following the same style as {@link InsertFilterWithMetricsTest}.
 */
class SchemaEvolutionFilterTest {

    // ── Pass-through cases ────────────────────────────────────────────────────

    @Test
    void validInsert_passesThrough() {
        assertNull(SchemaEvolutionFilter.classify(insertWithUser("u1")));
    }

    @Test
    void updateOp_passesThroughEvenWithoutUserId() {
        CdcEvent event = new CdcEvent();
        event.op   = "u";
        event.tsMs = 1700000000000L;
        event.after = new CdcEvent.UserEventRow(); // userId == null
        event.schemaVersion = SchemaVersion.V1;
        assertNull(SchemaEvolutionFilter.classify(event));
    }

    @Test
    void deleteOp_passesThrough() {
        CdcEvent event = new CdcEvent();
        event.op   = "d";
        event.tsMs = 1700000000000L;
        event.after = null;
        event.schemaVersion = SchemaVersion.UNKNOWN;
        assertNull(SchemaEvolutionFilter.classify(event));
    }

    @Test
    void wideningChange_backwardCompatible_passesThrough() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("created_at", "int", "long/bigint", ChangeType.WIDEN_TYPE));
        assertNull(SchemaEvolutionFilter.classify(event),
                "WIDEN_TYPE is BACKWARD compatible — should not route to DLQ");
    }

    @Test
    void addColumnChange_fullCompatible_passesThrough() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("session_id", null, "string", ChangeType.ADD_COLUMN));
        assertNull(SchemaEvolutionFilter.classify(event));
    }

    // ── DLQ routing cases ─────────────────────────────────────────────────────

    @Test
    void insertMissingUserId_routedToDlq() {
        CdcEvent event = new CdcEvent();
        event.op   = "c";
        event.tsMs = 1700000000000L;
        event.after = new CdcEvent.UserEventRow(); // userId == null
        event.schemaVersion = SchemaVersion.UNKNOWN;

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(ChangeType.DROP_COLUMN, dlq.changeType);
        assertEquals(CompatibilityLevel.BREAKING, dlq.compatibilityLevel);
        assertEquals("user_id", dlq.affectedColumn);
    }

    @Test
    void snapshotReadMissingUserId_routedToDlq() {
        CdcEvent event = new CdcEvent();
        event.op   = "r";
        event.tsMs = 1700000000000L;
        event.after = new CdcEvent.UserEventRow();
        event.schemaVersion = SchemaVersion.V1;

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(ChangeType.DROP_COLUMN, dlq.changeType);
    }

    @Test
    void incompatibleTypeChange_routedToDlq() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("created_at", "int", "string", ChangeType.INCOMPATIBLE_TYPE));

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(ChangeType.INCOMPATIBLE_TYPE, dlq.changeType);
        assertEquals("created_at", dlq.affectedColumn);
        assertEquals(CompatibilityLevel.BREAKING, dlq.compatibilityLevel);
        assertTrue(dlq.errorMessage.contains("BREAKING"));
        assertTrue(dlq.errorMessage.contains("created_at"));
    }

    @Test
    void narrowTypeChange_routedToDlq() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("amount", "long", "int", ChangeType.NARROW_TYPE));

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(ChangeType.NARROW_TYPE, dlq.changeType);
        assertEquals("amount", dlq.affectedColumn);
    }

    @Test
    void dropRequiredColumn_routedToDlq() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("user_id", "string", null, ChangeType.DROP_COLUMN));

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(ChangeType.DROP_COLUMN, dlq.changeType);
    }

    @Test
    void dlqEntryCarriesSchemaVersion() {
        CdcEvent event = new CdcEvent();
        event.op   = "c";
        event.tsMs = 1700000000000L;
        event.after = new CdcEvent.UserEventRow();
        event.schemaVersion = SchemaVersion.V2;

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals(SchemaVersion.V2, dlq.schemaVersion);
    }

    @Test
    void firstBreakingChangeWins_whenMultipleDetected() {
        CdcEvent event = insertWithUser("u1");
        event.detectedChanges.add(
                new SchemaChange("col_a", "int", "string", ChangeType.INCOMPATIBLE_TYPE));
        event.detectedChanges.add(
                new SchemaChange("col_b", "long", "int", ChangeType.NARROW_TYPE));

        DeadLetterEvent dlq = SchemaEvolutionFilter.classify(event);

        assertNotNull(dlq);
        assertEquals("col_a", dlq.affectedColumn, "first BREAKING change takes priority");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static CdcEvent insertWithUser(String userId) {
        CdcEvent event = new CdcEvent();
        event.op   = "c";
        event.tsMs = 1700000000000L;
        event.after = new CdcEvent.UserEventRow();
        event.after.userId    = userId;
        event.after.eventType = "click";
        event.after.createdAt = 1700000000000L;
        event.schemaVersion   = SchemaVersion.V1;
        return event;
    }
}
