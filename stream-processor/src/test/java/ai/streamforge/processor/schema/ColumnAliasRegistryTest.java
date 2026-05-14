package ai.streamforge.processor.schema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ColumnAliasRegistryTest {

    // ── Built-ins ────────────────────────────────────────────────────────────

    @Test
    void builtinsResolveUidToUserId() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertEquals("user_id", r.resolve("uid"));
    }

    @Test
    void builtinsResolveTypeToEventType() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertEquals("event_type", r.resolve("type"));
    }

    @Test
    void builtinsResolveTsToCreatedAt() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertEquals("created_at", r.resolve("ts"));
    }

    @Test
    void canonicalFieldReturnsItself() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertEquals("user_id", r.resolve("user_id"));
        assertEquals("event_type", r.resolve("event_type"));
        assertEquals("created_at", r.resolve("created_at"));
    }

    @Test
    void unknownFieldReturnsItself() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertEquals("some_random_column", r.resolve("some_random_column"));
    }

    // ── Registration ─────────────────────────────────────────────────────────

    @Test
    void registerCustomAlias() {
        ColumnAliasRegistry r = ColumnAliasRegistry.empty();
        r.register("customer_id", "user_id");
        assertEquals("user_id", r.resolve("customer_id"));
    }

    @Test
    void registerRejectsBlankInputs() {
        ColumnAliasRegistry r = ColumnAliasRegistry.empty();
        assertThrows(IllegalArgumentException.class, () -> r.register("", "user_id"));
        assertThrows(IllegalArgumentException.class, () -> r.register("uid", ""));
        assertThrows(IllegalArgumentException.class, () -> r.register(null, "user_id"));
    }

    @Test
    void isAliasReturnsTrueForRegisteredAlias() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertTrue(r.isAlias("uid"));
        assertFalse(r.isAlias("user_id"));
    }

    // ── Env loading ──────────────────────────────────────────────────────────

    @Test
    void loadFromEnvParsesValidSpec() {
        ColumnAliasRegistry r = ColumnAliasRegistry.empty();
        r.loadFromEnv("customer_id=user_id,kind=event_type");
        assertEquals("user_id",    r.resolve("customer_id"));
        assertEquals("event_type", r.resolve("kind"));
    }

    @Test
    void loadFromEnvSkipsMalformedEntries() {
        ColumnAliasRegistry r = ColumnAliasRegistry.empty();
        r.loadFromEnv("customer_id=user_id,MALFORMED,=empty_key");
        assertEquals("user_id", r.resolve("customer_id"));
        assertEquals("MALFORMED", r.resolve("MALFORMED")); // unchanged
    }

    @Test
    void loadFromEnvHandlesNullAndBlank() {
        ColumnAliasRegistry r = ColumnAliasRegistry.empty();
        assertDoesNotThrow(() -> r.loadFromEnv(null));
        assertDoesNotThrow(() -> r.loadFromEnv(""));
        assertDoesNotThrow(() -> r.loadFromEnv("   "));
    }

    @Test
    void allAliasesIsUnmodifiable() {
        ColumnAliasRegistry r = ColumnAliasRegistry.builtinsOnly();
        assertThrows(UnsupportedOperationException.class,
                () -> r.allAliases().put("bad", "injection"));
    }
}
