package ai.streamforge.processor.schema;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Classifies the type compatibility of a JSON field value relative to an
 * expected type hint, and determines whether a numeric value overflows an
 * assumed narrower type.
 *
 * <h3>Why in the deserializer, not in DDL</h3>
 * Debezium embeds type information in the schema history topic, not in each
 * data event.  The stream processor does not consume the schema history topic.
 * This class therefore operates on the runtime JSON node shape:
 * <ul>
 *   <li>Integer node (fits in int) → compatible with INT columns</li>
 *   <li>Integer node (overflows int) → indicates WIDEN_TYPE (BIGINT appeared)</li>
 *   <li>Integer node where a string was expected → INCOMPATIBLE_TYPE</li>
 * </ul>
 *
 * <h3>Widening rules</h3>
 * <pre>
 *   INT    → BIGINT   : value.longValue() &gt; Integer.MAX_VALUE  → WIDEN_TYPE (BACKWARD)
 *   FLOAT  → DOUBLE   : always safe; double precision is a superset
 *   TINYINT→ SMALLINT : value fits; always safe
 * </pre>
 *
 * <h3>Narrowing / incompatible rules</h3>
 * <pre>
 *   STRING → numeric  : INCOMPATIBLE_TYPE  (BREAKING)
 *   numeric→ STRING   : INCOMPATIBLE_TYPE  (BREAKING)
 *   BIGINT → INT with overflow : NARROW_TYPE (BREAKING)
 * </pre>
 */
public final class TypeCompatibilityChecker {

    private TypeCompatibilityChecker() {}

    /**
     * Examines {@code node} against the {@code expectedKind} for the given
     * {@code column} and returns a {@link SchemaChange} when a type mismatch
     * or widening is detected.
     *
     * @param column       column name (for diagnostics)
     * @param node         JSON node containing the current value
     * @param expectedKind one of "int", "long", "float", "double", "string"
     * @return a {@link SchemaChange} if a type deviation is detected, or {@code null}
     *         if the value is compatible with the expected kind
     */
    public static SchemaChange check(String column, JsonNode node, String expectedKind) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null; // absent/null — handled as ADD_COLUMN or DROP_COLUMN elsewhere
        }

        return switch (expectedKind.toLowerCase()) {
            case "int"    -> checkInt(column, node);
            case "long"   -> checkLong(column, node);
            case "float"  -> checkFloat(column, node);
            case "double" -> checkDouble(column, node);
            case "string" -> checkString(column, node);
            default       -> null;
        };
    }

    /**
     * Returns {@code true} if moving from {@code oldKind} to {@code newKind}
     * is a safe widening (BACKWARD compatible).
     */
    public static boolean isWidening(String oldKind, String newKind) {
        return switch (oldKind.toLowerCase()) {
            case "tinyint"  -> isOneOf(newKind, "smallint", "int", "long", "bigint", "float", "double");
            case "smallint" -> isOneOf(newKind, "int", "long", "bigint", "float", "double");
            case "int"      -> isOneOf(newKind, "long", "bigint", "float", "double");
            case "long", "bigint" -> isOneOf(newKind, "double");
            case "float"    -> isOneOf(newKind, "double");
            default         -> false;
        };
    }

    // ── Per-kind checks ──────────────────────────────────────────────────────

    private static SchemaChange checkInt(String column, JsonNode node) {
        if (node.isNumber()) {
            long v = node.longValue();
            if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
                return new SchemaChange(column, "int", "long/bigint", ChangeType.WIDEN_TYPE);
            }
            return null; // fits in int — fine
        }
        if (node.isTextual()) {
            return new SchemaChange(column, "int", "string", ChangeType.INCOMPATIBLE_TYPE);
        }
        return null;
    }

    private static SchemaChange checkLong(String column, JsonNode node) {
        if (node.isNumber()) {
            return null; // all numeric values fit in long
        }
        if (node.isTextual()) {
            return new SchemaChange(column, "long", "string", ChangeType.INCOMPATIBLE_TYPE);
        }
        return null;
    }

    private static SchemaChange checkFloat(String column, JsonNode node) {
        if (node.isFloatingPointNumber()) {
            return null; // fine
        }
        if (node.isIntegralNumber()) {
            return null; // integral widened to float — safe
        }
        if (node.isTextual()) {
            return new SchemaChange(column, "float", "string", ChangeType.INCOMPATIBLE_TYPE);
        }
        return null;
    }

    private static SchemaChange checkDouble(String column, JsonNode node) {
        if (node.isNumber()) {
            return null; // all numeric values fit in double
        }
        if (node.isTextual()) {
            return new SchemaChange(column, "double", "string", ChangeType.INCOMPATIBLE_TYPE);
        }
        return null;
    }

    private static SchemaChange checkString(String column, JsonNode node) {
        if (node.isTextual()) {
            return null; // fine
        }
        if (node.isNumber()) {
            return new SchemaChange(column, "string", "numeric", ChangeType.INCOMPATIBLE_TYPE);
        }
        return null;
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static boolean isOneOf(String value, String... options) {
        String v = value.toLowerCase();
        for (String o : options) {
            if (v.equals(o)) return true;
        }
        return false;
    }
}
