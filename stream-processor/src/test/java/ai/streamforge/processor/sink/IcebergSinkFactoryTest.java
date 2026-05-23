package ai.streamforge.processor.sink;

import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link IcebergSinkFactory} — schema shape, partition spec,
 * RowData mapping, and date derivation.
 *
 * No Flink or MinIO runtime required; all assertions operate on static methods
 * and pure value logic.
 */
class IcebergSinkFactoryTest {

    // ── Schema ────────────────────────────────────────────────────────────────

    @Test
    void schema_hasFiveFields() {
        Schema schema = IcebergSinkFactory.TABLE_SCHEMA;
        assertEquals(5, schema.columns().size());
    }

    @Test
    void schema_fieldIds_areStable() {
        Schema schema = IcebergSinkFactory.TABLE_SCHEMA;
        assertEquals(1, schema.findField("user_id").fieldId());
        assertEquals(2, schema.findField("event_count").fieldId());
        assertEquals(3, schema.findField("window_start_ms").fieldId());
        assertEquals(4, schema.findField("window_end_ms").fieldId());
        assertEquals(5, schema.findField("event_date").fieldId());
    }

    @Test
    void schema_allFieldsRequired() {
        IcebergSinkFactory.TABLE_SCHEMA.columns()
                .forEach(f -> assertTrue(f.isRequired(),
                        "Expected " + f.name() + " to be required"));
    }

    @Test
    void schema_eventDate_isString() {
        Types.NestedField field = IcebergSinkFactory.TABLE_SCHEMA.findField("event_date");
        assertNotNull(field);
        assertEquals(Types.StringType.get(), field.type());
    }

    // ── Partition spec ────────────────────────────────────────────────────────

    @Test
    void partitionSpec_hasOneField() {
        PartitionSpec spec = IcebergSinkFactory.PARTITION_SPEC;
        assertEquals(1, spec.fields().size());
    }

    @Test
    void partitionSpec_isIdentityOnEventDate() {
        PartitionSpec spec = IcebergSinkFactory.PARTITION_SPEC;
        PartitionField pf  = spec.fields().get(0);
        assertEquals("event_date", pf.name());
        assertEquals(Transforms.identity(), pf.transform());
    }

    @Test
    void partitionSpec_sourceFieldId_matchesSchemaEventDate() {
        PartitionSpec spec  = IcebergSinkFactory.PARTITION_SPEC;
        int specSourceId    = spec.fields().get(0).sourceId();
        int schemaFieldId   = IcebergSinkFactory.TABLE_SCHEMA.findField("event_date").fieldId();
        assertEquals(schemaFieldId, specSourceId);
    }

    // ── toEventDate ───────────────────────────────────────────────────────────

    @Test
    void toEventDate_returnsIsoDate() {
        // 2024-01-15 00:00:00 UTC in epoch-ms
        long epochMs = LocalDate.of(2024, 1, 15)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        assertEquals("2024-01-15", IcebergSinkFactory.toEventDate(epochMs));
    }

    @Test
    void toEventDate_justBeforeMidnight_staysInSameDay() {
        // 2024-03-10 23:59:59.999 UTC
        long epochMs = LocalDate.of(2024, 3, 10)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli() + 86_399_999L;
        assertEquals("2024-03-10", IcebergSinkFactory.toEventDate(epochMs));
    }

    @Test
    void toEventDate_atMidnight_advancesToNextDay() {
        // Exactly 2024-03-11 00:00:00.000 UTC
        long epochMs = LocalDate.of(2024, 3, 11)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        assertEquals("2024-03-11", IcebergSinkFactory.toEventDate(epochMs));
    }

    // ── toRowData ─────────────────────────────────────────────────────────────

    @Test
    void toRowData_hasFiveFields() {
        RowData row = IcebergSinkFactory.toRowData(sampleEvent());
        // GenericRowData.getArity() is the field count
        assertEquals(5, row.getArity());
    }

    @Test
    void toRowData_userId_isCorrect() {
        RowData row = IcebergSinkFactory.toRowData(sampleEvent());
        assertEquals(StringData.fromString("user-42"), row.getString(0));
    }

    @Test
    void toRowData_eventCount_isCorrect() {
        RowData row = IcebergSinkFactory.toRowData(sampleEvent());
        assertEquals(7L, row.getLong(1));
    }

    @Test
    void toRowData_windowStartMs_isCorrect() {
        UserEventCount e = sampleEvent();
        RowData row = IcebergSinkFactory.toRowData(e);
        assertEquals(e.windowStartMs, row.getLong(2));
    }

    @Test
    void toRowData_windowEndMs_isCorrect() {
        UserEventCount e = sampleEvent();
        RowData row = IcebergSinkFactory.toRowData(e);
        assertEquals(e.windowEndMs, row.getLong(3));
    }

    @Test
    void toRowData_eventDate_matchesToEventDate() {
        UserEventCount e = sampleEvent();
        RowData row = IcebergSinkFactory.toRowData(e);
        String expected = IcebergSinkFactory.toEventDate(e.windowStartMs);
        assertEquals(StringData.fromString(expected), row.getString(4));
    }

    @Test
    void toRowData_eventDate_isDateOfWindowStart_notWindowEnd() {
        // windowStart on 2024-06-30, windowEnd crosses midnight into 2024-07-01
        long startMs = LocalDate.of(2024, 6, 30)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + 3_600_000L; // 01:00 UTC
        long endMs   = startMs + 3_600_000L; // 02:00 UTC, same day
        UserEventCount e = new UserEventCount("u", 1, startMs, endMs);

        RowData row = IcebergSinkFactory.toRowData(e);
        assertEquals(StringData.fromString("2024-06-30"), row.getString(4));
    }

    // ── buildWriteProperties ──────────────────────────────────────────────────

    @Test
    void buildWriteProperties_containsRequiredKeys() {
        var props = IcebergSinkFactory.buildWriteProperties();
        assertTrue(props.containsKey("write.target-file-size-bytes"));
        assertTrue(props.containsKey("write.format.default"));
        assertTrue(props.containsKey("write.parquet.row-group-size-bytes"));
        assertTrue(props.containsKey("write.parquet.page-size-bytes"));
    }

    @Test
    void buildWriteProperties_defaultFormat_isParquet() {
        assertEquals("parquet", IcebergSinkFactory.buildWriteProperties()
                .get("write.format.default"));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static UserEventCount sampleEvent() {
        long startMs = LocalDate.of(2024, 6, 1)
                .atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        return new UserEventCount("user-42", 7L, startMs, startMs + 60_000L);
    }
}
