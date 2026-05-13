package ai.streamforge.processor.deserialization;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.SchemaVersion;
import ai.streamforge.processor.schema.ChangeType;
import ai.streamforge.processor.schema.ColumnAliasRegistry;
import ai.streamforge.processor.schema.SchemaChange;
import ai.streamforge.processor.schema.TypeCompatibilityChecker;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Stateless utility that normalizes a raw Debezium JSON payload into a
 * {@link CdcEvent}, handling schema evolution across four change categories:
 *
 * <h3>1. Column add</h3>
 * New nullable columns ({@code session_id}, {@code ip_address}, {@code metadata})
 * are silently null-filled for events that pre-date the DDL change.
 * Compatibility: <b>FULL</b>.
 *
 * <h3>2. Column drop</h3>
 * A dropped column simply becomes absent in the JSON — the row field is null-filled.
 * If the dropped column was a <em>required</em> field ({@code user_id}) the event is
 * flagged with {@link ChangeType#DROP_COLUMN} and routed to the DLQ by
 * {@link SchemaEvolutionFilter}.
 * Compatibility: <b>FORWARD</b> (optional) / <b>BREAKING</b> (required).
 *
 * <h3>3. Column rename</h3>
 * The {@link ColumnAliasRegistry} maps legacy names to canonical names.
 * Built-in aliases: {@code uid → user_id}, {@code type → event_type},
 * {@code ts → created_at}.  Additional aliases may be registered at runtime
 * via the {@code COLUMN_ALIASES} env var ({@code oldName=newName,...}).
 * An unregistered rename is treated as {@code DROP_COLUMN} on the canonical field
 * (routed to DLQ if required).
 * Compatibility: <b>BACKWARD</b> (with alias) / <b>BREAKING</b> (without).
 *
 * <h3>4. Type widening</h3>
 * Numeric widenings (e.g. INT → BIGINT for {@code created_at}) are detected by
 * {@link TypeCompatibilityChecker}.  The event is annotated and passes through.
 * Incompatible type changes (numeric → string) are routed to the DLQ.
 * Compatibility: <b>BACKWARD</b> (widening) / <b>BREAKING</b> (incompatible).
 *
 * <h3>Schema version detection</h3>
 * <ul>
 *   <li><b>V1</b> — only core fields: {@code user_id}/{@code uid}, {@code event_type},
 *       {@code created_at}.</li>
 *   <li><b>V2</b> — adds {@code session_id} and/or {@code ip_address}.</li>
 *   <li><b>V3</b> — adds {@code metadata} blob or {@code created_at} overflows int range.</li>
 *   <li><b>UNKNOWN</b> — {@code after} absent or no known key field found.</li>
 * </ul>
 */
public final class SchemaEvolutionHandler {

    private SchemaEvolutionHandler() {}

    /**
     * Parses {@code bytes} into a {@link CdcEvent} with full schema normalization.
     * Uses the default alias registry (built-ins + {@code COLUMN_ALIASES} env var).
     *
     * @throws IOException if the bytes are not valid JSON
     */
    public static CdcEvent handle(byte[] bytes, ObjectMapper mapper) throws IOException {
        return handle(bytes, mapper, ColumnAliasRegistry.withDefaults());
    }

    /**
     * Parses {@code bytes} using a caller-supplied {@link ColumnAliasRegistry}.
     * Useful in tests to supply a controlled alias set.
     *
     * @throws IOException if the bytes are not valid JSON
     */
    public static CdcEvent handle(
            byte[] bytes, ObjectMapper mapper, ColumnAliasRegistry aliases) throws IOException {
        JsonNode root = mapper.readTree(bytes);

        CdcEvent event = new CdcEvent();
        event.op   = textOrNull(root, "op");
        event.tsMs = root.path("ts_ms").asLong(0L);

        JsonNode afterNode = root.path("after");
        if (!afterNode.isMissingNode() && !afterNode.isNull()) {
            List<SchemaChange> changes = new ArrayList<>();
            event.after           = normalizeRow(afterNode, aliases, changes);
            event.detectedChanges = changes;
            event.schemaVersion   = detectVersion(afterNode, event.after);
        } else {
            event.schemaVersion = SchemaVersion.UNKNOWN;
        }

        return event;
    }

    // ── Version detection ────────────────────────────────────────────────────

    static SchemaVersion detectVersion(JsonNode afterNode, CdcEvent.UserEventRow row) {
        // V3: metadata blob present, or created_at overflows 32-bit int range
        if (afterNode.has("metadata")
                || (row.createdAt != null
                    && (row.createdAt > Integer.MAX_VALUE || row.createdAt < Integer.MIN_VALUE))) {
            return SchemaVersion.V3;
        }
        // V2: session_id or ip_address present
        if (afterNode.has("session_id") || afterNode.has("ip_address")) {
            return SchemaVersion.V2;
        }
        // V1: at least one core canonical field (or its alias) resolved
        if (row.userId != null || row.eventType != null) {
            return SchemaVersion.V1;
        }
        return SchemaVersion.UNKNOWN;
    }

    // ── Field normalization ──────────────────────────────────────────────────

    static CdcEvent.UserEventRow normalizeRow(
            JsonNode afterNode, ColumnAliasRegistry aliases, List<SchemaChange> changes) {
        CdcEvent.UserEventRow row = new CdcEvent.UserEventRow();

        // user_id — accepts registered aliases (built-in: uid)
        row.userId = resolveText(afterNode, aliases, "user_id");
        if (row.userId == null && !hasFieldOrAlias(afterNode, aliases, "user_id")) {
            // Required field absent — signal a DROP of a required column
            changes.add(new SchemaChange("user_id", "string", null, ChangeType.DROP_COLUMN));
        }

        // event_type — accepts registered aliases (built-in: type)
        row.eventType = resolveText(afterNode, aliases, "event_type");

        // created_at — accepts registered aliases (built-in: ts); check for type widening
        row.createdAt = resolveTimestamp(afterNode, aliases, changes, "created_at");

        // V2 fields — null when absent (compatible add; no change recorded)
        row.sessionId = textOrNull(afterNode, "session_id");
        row.ipAddress = textOrNull(afterNode, "ip_address");

        // V3 fields — null when absent (compatible add)
        row.metadata = textOrNull(afterNode, "metadata");

        return row;
    }

    // ── Resolution helpers ───────────────────────────────────────────────────

    /**
     * Returns the text value of {@code canonical} or any alias that maps to it.
     * Canonical field takes priority over aliases.
     */
    static String resolveText(JsonNode node, ColumnAliasRegistry aliases, String canonical) {
        // Try the canonical name first
        String val = textOrNull(node, canonical);
        if (val != null) return val;

        // Try every registered alias that maps to this canonical
        for (java.util.Map.Entry<String, String> entry : aliases.allAliases().entrySet()) {
            if (canonical.equals(entry.getValue())) {
                val = textOrNull(node, entry.getKey());
                if (val != null) return val;
            }
        }
        return null;
    }

    /**
     * Returns the long timestamp for {@code canonical} or any registered alias.
     * Appends a {@link ChangeType#WIDEN_TYPE} or {@link ChangeType#INCOMPATIBLE_TYPE}
     * change to {@code changes} if the value deviates from the expected int type.
     */
    static Long resolveTimestamp(
            JsonNode node,
            ColumnAliasRegistry aliases,
            List<SchemaChange> changes,
            String canonical) {
        JsonNode child = node.get(canonical);
        if (child == null || child.isNull()) {
            // Try aliases
            for (java.util.Map.Entry<String, String> entry : aliases.allAliases().entrySet()) {
                if (canonical.equals(entry.getValue())) {
                    child = node.get(entry.getKey());
                    if (child != null && !child.isNull()) break;
                }
            }
        }
        if (child == null || child.isNull()) return null;

        SchemaChange typeChange = TypeCompatibilityChecker.check(canonical, child, "int");
        if (typeChange != null) {
            changes.add(typeChange);
        }
        return child.asLong();
    }

    /** Returns {@code true} if the canonical field or any alias mapping to it is present in the node. */
    static boolean hasFieldOrAlias(JsonNode node, ColumnAliasRegistry aliases, String canonical) {
        if (node.has(canonical)) return true;
        for (java.util.Map.Entry<String, String> entry : aliases.allAliases().entrySet()) {
            if (canonical.equals(entry.getValue()) && node.has(entry.getKey())) return true;
        }
        return false;
    }

    static String textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        return (child != null && !child.isNull()) ? child.asText() : null;
    }
}
