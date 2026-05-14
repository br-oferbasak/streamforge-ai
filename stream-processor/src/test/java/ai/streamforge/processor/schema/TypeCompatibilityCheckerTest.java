package ai.streamforge.processor.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TypeCompatibilityCheckerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    // ── INT checks ───────────────────────────────────────────────────────────

    @Test
    void intValueFitsInInt_noChange() {
        assertNull(TypeCompatibilityChecker.check("col", IntNode.valueOf(42), "int"));
    }

    @Test
    void intValueOverflowsInt_detectsWidening() {
        long bigValue = (long) Integer.MAX_VALUE + 1;
        SchemaChange change = TypeCompatibilityChecker.check("ts", LongNode.valueOf(bigValue), "int");
        assertNotNull(change);
        assertEquals(ChangeType.WIDEN_TYPE, change.changeType);
        assertEquals("ts", change.column);
        assertEquals(CompatibilityLevel.BACKWARD, change.effectiveCompatibility);
    }

    @Test
    void stringWhereIntExpected_detectsIncompatible() {
        SchemaChange change = TypeCompatibilityChecker.check("ts", TextNode.valueOf("2024-01-01"), "int");
        assertNotNull(change);
        assertEquals(ChangeType.INCOMPATIBLE_TYPE, change.changeType);
        assertEquals(CompatibilityLevel.BREAKING, change.effectiveCompatibility);
    }

    // ── LONG checks ──────────────────────────────────────────────────────────

    @Test
    void anyNumericFitsInLong_noChange() {
        assertNull(TypeCompatibilityChecker.check("col", LongNode.valueOf(Long.MAX_VALUE), "long"));
    }

    @Test
    void stringWhereIntLongExpected_detectsIncompatible() {
        SchemaChange change = TypeCompatibilityChecker.check("col", TextNode.valueOf("abc"), "long");
        assertNotNull(change);
        assertEquals(ChangeType.INCOMPATIBLE_TYPE, change.changeType);
    }

    // ── STRING checks ────────────────────────────────────────────────────────

    @Test
    void textNodeWhereStringExpected_noChange() {
        assertNull(TypeCompatibilityChecker.check("name", TextNode.valueOf("Alice"), "string"));
    }

    @Test
    void numericWhereStringExpected_detectsIncompatible() {
        SchemaChange change = TypeCompatibilityChecker.check("name", IntNode.valueOf(123), "string");
        assertNotNull(change);
        assertEquals(ChangeType.INCOMPATIBLE_TYPE, change.changeType);
    }

    // ── FLOAT / DOUBLE ───────────────────────────────────────────────────────

    @Test
    void floatNode_noChangeForFloatExpected() throws Exception {
        var node = mapper.readTree("3.14");
        assertNull(TypeCompatibilityChecker.check("col", node, "float"));
    }

    @Test
    void doubleNode_noChangeForDoubleExpected() throws Exception {
        var node = mapper.readTree("3.141592653589793");
        assertNull(TypeCompatibilityChecker.check("col", node, "double"));
    }

    // ── Null / missing ───────────────────────────────────────────────────────

    @Test
    void nullNode_returnsNull() throws Exception {
        var node = mapper.readTree("null");
        assertNull(TypeCompatibilityChecker.check("col", node, "int"));
    }

    @Test
    void nullReference_returnsNull() {
        assertNull(TypeCompatibilityChecker.check("col", null, "int"));
    }

    // ── isWidening ───────────────────────────────────────────────────────────

    @Test
    void intToLong_isWidening() {
        assertTrue(TypeCompatibilityChecker.isWidening("int", "long"));
    }

    @Test
    void intToDouble_isWidening() {
        assertTrue(TypeCompatibilityChecker.isWidening("int", "double"));
    }

    @Test
    void floatToDouble_isWidening() {
        assertTrue(TypeCompatibilityChecker.isWidening("float", "double"));
    }

    @Test
    void longToInt_isNotWidening() {
        assertFalse(TypeCompatibilityChecker.isWidening("long", "int"));
    }

    @Test
    void stringToInt_isNotWidening() {
        assertFalse(TypeCompatibilityChecker.isWidening("string", "int"));
    }
}
