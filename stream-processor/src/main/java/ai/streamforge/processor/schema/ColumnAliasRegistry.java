package ai.streamforge.processor.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registry that maps legacy or renamed column names to their current canonical names.
 *
 * <h3>Purpose</h3>
 * When Debezium captures a column rename ({@code ALTER TABLE … CHANGE COLUMN}),
 * events written before the rename carry the old column name and events written
 * after carry the new one.  Registering an alias lets the deserialization layer
 * normalize both to the same canonical field name, turning a BREAKING rename into
 * a transparent BACKWARD-compatible migration.
 *
 * <h3>Built-in aliases</h3>
 * <ul>
 *   <li>{@code uid}  → {@code user_id}   (historical rename)</li>
 *   <li>{@code type} → {@code event_type} (historical rename)</li>
 *   <li>{@code ts}   → {@code created_at} (historical rename)</li>
 * </ul>
 *
 * <h3>Runtime registration</h3>
 * New aliases can be added at runtime via {@link #register(String, String)} or
 * bulk-loaded from the environment variable {@code COLUMN_ALIASES} (format:
 * {@code alias1=canonical1,alias2=canonical2}).  This allows operators to deploy
 * an alias without recompiling or restarting the Flink job from scratch — a
 * savepoint-restore cycle is sufficient.
 */
public final class ColumnAliasRegistry implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ColumnAliasRegistry.class);

    /** alias → canonical column name */
    private final Map<String, String> aliases;

    // ── Factory ──────────────────────────────────────────────────────────────

    /** Returns a registry pre-loaded with built-in aliases and any from the env var. */
    public static ColumnAliasRegistry withDefaults() {
        ColumnAliasRegistry registry = new ColumnAliasRegistry();
        registry.registerBuiltins();
        registry.loadFromEnv(System.getenv("COLUMN_ALIASES"));
        return registry;
    }

    /** Returns a registry with *only* the built-in aliases (useful in tests). */
    public static ColumnAliasRegistry builtinsOnly() {
        ColumnAliasRegistry registry = new ColumnAliasRegistry();
        registry.registerBuiltins();
        return registry;
    }

    /** Returns an empty registry (no built-ins). */
    public static ColumnAliasRegistry empty() {
        return new ColumnAliasRegistry();
    }

    private ColumnAliasRegistry() {
        this.aliases = new LinkedHashMap<>();
    }

    // ── Registration ─────────────────────────────────────────────────────────

    /**
     * Registers {@code alias} as an alternative name for {@code canonical}.
     *
     * @param alias     the old / alternative column name that may appear in events
     * @param canonical the authoritative column name used by the processing logic
     */
    public void register(String alias, String canonical) {
        if (alias == null || canonical == null || alias.isBlank() || canonical.isBlank()) {
            throw new IllegalArgumentException("alias and canonical must be non-blank");
        }
        String prev = aliases.put(alias.trim(), canonical.trim());
        if (prev != null && !prev.equals(canonical)) {
            LOG.warn("Column alias '{}' re-registered: '{}' → '{}'", alias, prev, canonical);
        }
    }

    /**
     * Parses and loads aliases from {@code spec}.
     * Format: {@code alias1=canonical1,alias2=canonical2}.
     * Silently ignores malformed entries.
     */
    public void loadFromEnv(String spec) {
        if (spec == null || spec.isBlank()) {
            return;
        }
        for (String pair : spec.split(",")) {
            String[] parts = pair.split("=", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                register(parts[0].trim(), parts[1].trim());
                LOG.info("Loaded column alias from env: '{}' → '{}'", parts[0].trim(), parts[1].trim());
            } else {
                LOG.warn("Skipping malformed COLUMN_ALIASES entry: '{}'", pair);
            }
        }
    }

    // ── Lookup ───────────────────────────────────────────────────────────────

    /**
     * Resolves {@code columnName} to its canonical name.
     * If {@code columnName} is itself canonical (or has no registered alias),
     * it is returned unchanged.
     */
    public String resolve(String columnName) {
        return aliases.getOrDefault(columnName, columnName);
    }

    /**
     * Returns the canonical name for the first non-null alternative found in
     * {@code candidates}.  Useful for coalescing across multiple possible names.
     *
     * @return the first non-null candidate after alias resolution, or {@code null}
     */
    public String resolveFirstPresent(Map<String, Object> row, String... candidates) {
        for (String candidate : candidates) {
            if (row.containsKey(candidate)) {
                return candidate;
            }
            // check if any registered alias resolves to this candidate
            for (Map.Entry<String, String> e : aliases.entrySet()) {
                if (e.getValue().equals(candidate) && row.containsKey(e.getKey())) {
                    return e.getKey();
                }
            }
        }
        return null;
    }

    /** Returns {@code true} if the registry knows about this alias. */
    public boolean isAlias(String columnName) {
        return aliases.containsKey(columnName);
    }

    /** Returns an unmodifiable view of all registered aliases for diagnostics. */
    public Map<String, String> allAliases() {
        return Collections.unmodifiableMap(aliases);
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private void registerBuiltins() {
        register("uid",  "user_id");
        register("type", "event_type");
        register("ts",   "created_at");
    }
}
