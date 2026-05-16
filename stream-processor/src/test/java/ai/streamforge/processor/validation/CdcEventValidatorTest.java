package ai.streamforge.processor.validation;

import ai.streamforge.processor.model.CdcEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CdcEventValidatorTest {

    // A timestamp well within the valid range (2024-01-01)
    private static final long VALID_TS = 1_704_067_200_000L;

    private CdcEventValidator validator;
    private long nowMs;

    @BeforeEach
    void setUp() {
        nowMs     = System.currentTimeMillis();
        validator = new CdcEventValidator(3_600_000L, 128);
    }

    // ── Schema: op-code ──────────────────────────────────────────────────────

    @Test
    void validOp_insert_passes() {
        assertNull(validator.validate(insertEvent("u1", "click"), nowMs));
    }

    @Test
    void validOp_snapshotRead_passes() {
        CdcEvent e = insertEvent("u1", "click");
        e.op = "r";
        assertNull(validator.validate(e, nowMs));
    }

    @Test
    void validOp_update_passes() {
        CdcEvent e = new CdcEvent();
        e.op  = "u";
        e.tsMs = VALID_TS;
        assertNull(validator.validate(e, nowMs));
    }

    @Test
    void validOp_delete_passes() {
        CdcEvent e = new CdcEvent();
        e.op   = "d";
        e.tsMs = VALID_TS;
        assertNull(validator.validate(e, nowMs));
    }

    @Test
    void invalidOp_null_quarantined() {
        CdcEvent e = new CdcEvent();
        e.op   = null;
        e.tsMs = VALID_TS;
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.SCHEMA_MISMATCH.label()));
    }

    @Test
    void invalidOp_unknown_quarantined() {
        CdcEvent e = new CdcEvent();
        e.op   = "x";
        e.tsMs = VALID_TS;
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.SCHEMA_MISMATCH.label()));
    }

    // ── Range: timestamp ─────────────────────────────────────────────────────

    @Test
    void tsMs_beforeFloor_quarantined() {
        CdcEvent e = insertEvent("u1", "click");
        e.tsMs = CdcEventValidator.TS_FLOOR_MS - 1;
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.RANGE_VIOLATION.label()));
        assertTrue(v.contains("before floor"));
    }

    @Test
    void tsMs_atFloor_passes() {
        CdcEvent e = insertEvent("u1", "click");
        e.tsMs = CdcEventValidator.TS_FLOOR_MS;
        assertNull(validator.validate(e, nowMs));
    }

    @Test
    void tsMs_tooFarFuture_quarantined() {
        CdcEvent e = insertEvent("u1", "click");
        e.tsMs = nowMs + 3_600_001L;   // 1 ms over the 1-hour skew allowance
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.RANGE_VIOLATION.label()));
        assertTrue(v.contains("in the future"));
    }

    @Test
    void tsMs_atSkewBoundary_passes() {
        CdcEvent e = insertEvent("u1", "click");
        e.tsMs = nowMs + 3_600_000L;   // exactly at the skew limit
        assertNull(validator.validate(e, nowMs));
    }

    // ── Null fields ──────────────────────────────────────────────────────────

    @Test
    void nullAfter_forInsert_quarantined() {
        CdcEvent e = new CdcEvent();
        e.op   = "c";
        e.tsMs = VALID_TS;
        e.after = null;
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.NULL_FIELD.label()));
        assertTrue(v.contains("after is null"));
    }

    @Test
    void nullUserId_quarantined() {
        CdcEvent e = insertEvent(null, "click");
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.NULL_FIELD.label()));
        assertTrue(v.contains("userId"));
    }

    @Test
    void blankUserId_quarantined() {
        CdcEvent e = insertEvent("  ", "click");
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.NULL_FIELD.label()));
    }

    @Test
    void nullEventType_quarantined() {
        CdcEvent e = insertEvent("u1", null);
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.NULL_FIELD.label()));
        assertTrue(v.contains("eventType"));
    }

    // ── Range: userId length ──────────────────────────────────────────────────

    @Test
    void userId_atMaxLen_passes() {
        CdcEvent e = insertEvent("a".repeat(128), "click");
        assertNull(validator.validate(e, nowMs));
    }

    @Test
    void userId_overMaxLen_quarantined() {
        CdcEvent e = insertEvent("a".repeat(129), "click");
        String v = validator.validate(e, nowMs);
        assertNotNull(v);
        assertTrue(v.startsWith(ViolationType.RANGE_VIOLATION.label()));
        assertTrue(v.contains("exceeds max"));
    }

    @Test
    void update_and_delete_skip_payload_checks() {
        // op=u and op=d don't require an 'after' payload
        CdcEvent u = new CdcEvent();
        u.op   = "u";
        u.tsMs = VALID_TS;
        assertNull(validator.validate(u, nowMs));

        CdcEvent d = new CdcEvent();
        d.op   = "d";
        d.tsMs = VALID_TS;
        assertNull(validator.validate(d, nowMs));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static CdcEvent insertEvent(String userId, String eventType) {
        CdcEvent e = new CdcEvent();
        e.op   = "c";
        e.tsMs = VALID_TS;
        e.after = new CdcEvent.UserEventRow();
        e.after.userId    = userId;
        e.after.eventType = eventType;
        return e;
    }
}
