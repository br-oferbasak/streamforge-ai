package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.SchemaEvolutionHandler;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import ai.streamforge.processor.schema.ChangeType;
import ai.streamforge.processor.schema.ColumnAliasRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaEvolutionHandlerTest {

    private ObjectMapper mapper;
    private ColumnAliasRegistry aliases;

    @BeforeEach
    void setUp() {
        mapper  = new ObjectMapper();
        aliases = ColumnAliasRegistry.builtinsOnly();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Column ADD — FULL compatibility
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class AddColumn {

        @Test
        void v1EventMissingSessionId_nullFilled() throws Exception {
            String json = event("c", after("user_id", "u1", "event_type", "click",
                    "created_at", 1700000000000L));
            CdcEvent e = parse(json);
            assertEquals(SchemaVersion.V1, e.schemaVersion);
            assertNull(e.after.sessionId);
            assertNull(e.after.ipAddress);
            assertTrue(e.detectedChanges.isEmpty(), "no changes for missing optional field");
        }

        @Test
        void v2EventCarriesNewColumns() throws Exception {
            String json = event("c", afterV2("u1", "click", 1700000000000L, "sess-1", "10.0.0.1"));
            CdcEvent e = parse(json);
            assertEquals(SchemaVersion.V2, e.schemaVersion);
            assertEquals("sess-1",   e.after.sessionId);
            assertEquals("10.0.0.1", e.after.ipAddress);
            assertTrue(e.detectedChanges.isEmpty());
        }

        @Test
        void v3EventCarriesMetadataBlob() throws Exception {
            String json = """
                    {"op":"c","ts_ms":1700000000000,
                     "after":{"user_id":"u1","event_type":"click","created_at":1700000000000,
                               "session_id":"s1","ip_address":"1.2.3.4",
                               "metadata":"{\\"source\\":\\"web\\"}"}}
                    """;
            CdcEvent e = parse(json);
            assertEquals(SchemaVersion.V3, e.schemaVersion);
            assertNotNull(e.after.metadata);
            assertTrue(e.after.metadata.contains("web"));
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Column DROP
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class DropColumn {

        @Test
        void droppingOptionalEventType_nullFilledNoChange() throws Exception {
            String json = event("c", afterSimple("user_id", "u1"));
            CdcEvent e = parse(json);
            assertNull(e.after.eventType);
            // Only user_id is required; no drop change expected for optional field
            assertTrue(e.detectedChanges.isEmpty());
        }

        @Test
        void droppingRequiredUserId_detectsDropChange() throws Exception {
            String json = event("c",
                    "{\"event_type\":\"click\",\"created_at\":1700000000000}");
            CdcEvent e = parse(json);
            assertNull(e.after.userId);
            assertEquals(1, e.detectedChanges.size());
            assertEquals(ChangeType.DROP_COLUMN, e.detectedChanges.get(0).changeType);
            assertEquals("user_id", e.detectedChanges.get(0).column);
        }

        @Test
        void deleteOp_afterIsNull_noDropDetected() throws Exception {
            String json = "{\"op\":\"d\",\"ts_ms\":1700000000000,\"after\":null}";
            CdcEvent e = parse(json);
            assertEquals(SchemaVersion.UNKNOWN, e.schemaVersion);
            assertNull(e.after);
            assertTrue(e.detectedChanges.isEmpty());
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Column RENAME — via alias registry
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class RenameColumn {

        @Test
        void builtinAlias_uid_resolvesToUserId() throws Exception {
            String json = event("c",
                    "{\"uid\":\"user-renamed\",\"event_type\":\"view\",\"created_at\":1700000000000}");
            CdcEvent e = parse(json);
            assertEquals("user-renamed", e.after.userId);
            assertTrue(e.detectedChanges.isEmpty());
        }

        @Test
        void builtinAlias_type_resolvesToEventType() throws Exception {
            String json = event("c",
                    "{\"user_id\":\"u1\",\"type\":\"purchase\",\"created_at\":1700000000000}");
            CdcEvent e = parse(json);
            assertEquals("purchase", e.after.eventType);
        }

        @Test
        void builtinAlias_ts_resolvesToCreatedAt() throws Exception {
            String json = event("c",
                    "{\"user_id\":\"u1\",\"event_type\":\"click\",\"ts\":1699999999000}");
            CdcEvent e = parse(json);
            assertEquals(1699999999000L, e.after.createdAt);
        }

        @Test
        void canonicalFieldTakesPriorityOverAlias() throws Exception {
            String json = event("c",
                    "{\"user_id\":\"canonical\",\"uid\":\"alias\","
                    + "\"event_type\":\"click\",\"created_at\":1700000000000}");
            CdcEvent e = parse(json);
            assertEquals("canonical", e.after.userId);
        }

        @Test
        void runtimeAlias_customerIdResolvesToUserId() throws Exception {
            ColumnAliasRegistry custom = ColumnAliasRegistry.builtinsOnly();
            custom.register("customer_id", "user_id");

            String json = event("c",
                    "{\"customer_id\":\"cust-99\",\"event_type\":\"click\",\"created_at\":1700000000000}");
            CdcEvent e = SchemaEvolutionHandler.handle(json.getBytes(), mapper, custom);
            assertEquals("cust-99", e.after.userId);
            assertTrue(e.detectedChanges.isEmpty(), "alias registration makes rename transparent");
        }

        @Test
        void unregisteredRename_userId_absent_detectsDrop() throws Exception {
            // Column was renamed to 'account_id' but no alias registered
            String json = event("c",
                    "{\"account_id\":\"acc-1\",\"event_type\":\"click\",\"created_at\":1700000000000}");
            CdcEvent e = parse(json);
            assertNull(e.after.userId);
            assertEquals(1, e.detectedChanges.size());
            assertEquals(ChangeType.DROP_COLUMN, e.detectedChanges.get(0).changeType);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Type WIDENING — BACKWARD compatible
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class TypeWidening {

        @Test
        void createdAtFitsInInt_noWideningDetected() throws Exception {
            String json = event("c",
                    "{\"user_id\":\"u1\",\"event_type\":\"click\",\"created_at\":1000000}");
            CdcEvent e = parse(json);
            assertTrue(e.detectedChanges.isEmpty());
            assertEquals(1_000_000L, e.after.createdAt);
        }

        @Test
        void createdAtOverflowsInt_detectsWidening() throws Exception {
            long bigTs = (long) Integer.MAX_VALUE + 999;
            String json = event("c",
                    "{\"user_id\":\"u1\",\"event_type\":\"click\",\"created_at\":" + bigTs + "}");
            CdcEvent e = parse(json);
            assertEquals(1, e.detectedChanges.size());
            assertEquals(ChangeType.WIDEN_TYPE, e.detectedChanges.get(0).changeType);
            assertEquals("created_at", e.detectedChanges.get(0).column);
            assertEquals(bigTs, e.after.createdAt, "value still correctly parsed");
            assertEquals(SchemaVersion.V3, e.schemaVersion, "overflow triggers V3 detection");
        }

        @Test
        void createdAtWidenedAlsoViaAlias_ts() throws Exception {
            long bigTs = (long) Integer.MAX_VALUE + 1;
            String json = event("c",
                    "{\"user_id\":\"u1\",\"event_type\":\"click\",\"ts\":" + bigTs + "}");
            CdcEvent e = parse(json);
            assertEquals(1, e.detectedChanges.size());
            assertEquals(ChangeType.WIDEN_TYPE, e.detectedChanges.get(0).changeType);
            assertEquals(bigTs, e.after.createdAt);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Incompatible type change — BREAKING
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class IncompatibleType {

        @Test
        void createdAtIsString_detectsIncompatibleType() throws Exception {
            String json = event("c",
                    "{\"user_id\":\"u1\",\"event_type\":\"click\",\"created_at\":\"2024-01-01\"}");
            CdcEvent e = parse(json);
            assertEquals(1, e.detectedChanges.size());
            assertEquals(ChangeType.INCOMPATIBLE_TYPE, e.detectedChanges.get(0).changeType);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Envelope / misc
    // ═══════════════════════════════════════════════════════════════════════════
    @Nested
    class Envelope {

        @Test
        void parsesOpAndTsMs() throws Exception {
            String json = event("u", after("user_id", "u2", "event_type", "update",
                    "created_at", 1700001234000L));
            CdcEvent e = parse(json);
            assertEquals("u", e.op);
        }

        @Test
        void ignoresUnknownEnvelopeFields() throws Exception {
            String json = """
                    {"op":"c","ts_ms":1700000000000,
                     "source":{"connector":"mysql"},"transaction":null,
                     "after":{"user_id":"u1","event_type":"click","created_at":1700000000000}}
                    """;
            CdcEvent e = parse(json);
            assertEquals("u1", e.after.userId);
        }

        @Test
        void toleratesMissingOptionalFields() throws Exception {
            String json = event("c", afterSimple("user_id", "u1"));
            CdcEvent e = parse(json);
            assertNull(e.after.eventType);
            assertNull(e.after.createdAt);
            assertNull(e.after.sessionId);
        }

        @Test
        void afterIsNull_unknownVersion() throws Exception {
            String json = "{\"op\":\"d\",\"ts_ms\":1700000000000,\"after\":null}";
            CdcEvent e = parse(json);
            assertEquals(SchemaVersion.UNKNOWN, e.schemaVersion);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private CdcEvent parse(String json) throws Exception {
        return SchemaEvolutionHandler.handle(json.getBytes(), mapper, aliases);
    }

    private static String event(String op, String afterJson) {
        return "{\"op\":\"" + op + "\",\"ts_ms\":1700000000000,\"after\":" + afterJson + "}";
    }

    private static String after(String k1, Object v1, String k2, Object v2,
                                String k3, Object v3) {
        return "{\"" + k1 + "\":" + quote(v1) + ",\""
                + k2 + "\":" + quote(v2) + ",\""
                + k3 + "\":" + quote(v3) + "}";
    }

    private static String afterV2(String userId, String type, long ts, String session, String ip) {
        return "{\"user_id\":\"" + userId + "\",\"event_type\":\"" + type + "\","
                + "\"created_at\":" + ts + ",\"session_id\":\"" + session + "\","
                + "\"ip_address\":\"" + ip + "\"}";
    }

    private static String afterSimple(String k, Object v) {
        return "{\"" + k + "\":" + quote(v) + "}";
    }

    private static String quote(Object v) {
        return (v instanceof String) ? "\"" + v + "\"" : String.valueOf(v);
    }
}
