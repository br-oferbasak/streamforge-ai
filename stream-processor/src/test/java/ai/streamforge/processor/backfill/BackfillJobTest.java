package ai.streamforge.processor.backfill;

import ai.streamforge.processor.model.CdcEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BackfillJobTest {

    // ── BackfillMode.parse ───────────────────────────────────────────────────

    @Test
    void parse_kafkaRange_canonical() {
        assertEquals(BackfillMode.KAFKA_RANGE, BackfillMode.parse("kafka_range"));
    }

    @Test
    void parse_cdcSnapshot_canonical() {
        assertEquals(BackfillMode.CDC_SNAPSHOT, BackfillMode.parse("cdc_snapshot"));
    }

    @Test
    void parse_cdcSnapshot_hyphenVariant() {
        assertEquals(BackfillMode.CDC_SNAPSHOT, BackfillMode.parse("cdc-snapshot"));
    }

    @Test
    void parse_unknown_throws() {
        assertThrows(IllegalArgumentException.class, () -> BackfillMode.parse("full_replay"));
    }

    // ── acceptedOp ──────────────────────────────────────────────────────────

    @Test
    void acceptedOp_insert_acceptedInBothModes() {
        CdcEvent e = event("c");
        assertTrue(BackfillJob.acceptedOp(e, BackfillMode.KAFKA_RANGE));
        assertTrue(BackfillJob.acceptedOp(e, BackfillMode.CDC_SNAPSHOT));
    }

    @Test
    void acceptedOp_snapshotRead_onlyInCdcSnapshotMode() {
        CdcEvent e = event("r");
        assertFalse(BackfillJob.acceptedOp(e, BackfillMode.KAFKA_RANGE),
                "op=r must be rejected in kafka_range mode");
        assertTrue(BackfillJob.acceptedOp(e, BackfillMode.CDC_SNAPSHOT),
                "op=r must be accepted in cdc_snapshot mode");
    }

    @Test
    void acceptedOp_update_rejectedInBothModes() {
        CdcEvent e = event("u");
        assertFalse(BackfillJob.acceptedOp(e, BackfillMode.KAFKA_RANGE));
        assertFalse(BackfillJob.acceptedOp(e, BackfillMode.CDC_SNAPSHOT));
    }

    @Test
    void acceptedOp_delete_rejectedInBothModes() {
        CdcEvent e = event("d");
        assertFalse(BackfillJob.acceptedOp(e, BackfillMode.KAFKA_RANGE));
        assertFalse(BackfillJob.acceptedOp(e, BackfillMode.CDC_SNAPSHOT));
    }

    // ── configureOffsets validation ──────────────────────────────────────────

    @Test
    void configureOffsets_kafkaRange_requiresStartTimestamp() {
        // Ensure validation fires when BACKFILL_START_TIMESTAMP_MS is absent/zero.
        // We call directly so no env var is set; env() defaults to "0".
        assertThrows(IllegalArgumentException.class, () ->
                BackfillJob.configureOffsets(
                        org.apache.flink.connector.kafka.source.KafkaSource.<CdcEvent>builder()
                                .setBootstrapServers("localhost:9092")
                                .setTopics("test-topic")
                                .setGroupId("test-group")
                                .setValueOnlyDeserializer(
                                        new ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema()),
                        BackfillMode.KAFKA_RANGE));
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static CdcEvent event(String op) {
        CdcEvent e = new CdcEvent();
        e.op = op;
        return e;
    }
}
