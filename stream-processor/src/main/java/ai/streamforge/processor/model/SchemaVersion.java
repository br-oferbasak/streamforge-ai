package ai.streamforge.processor.model;

/**
 * Tracks the schema version of an incoming {@link CdcEvent}, detected from
 * which fields are present in the Debezium {@code after} payload.
 *
 * <h3>Version history</h3>
 * <ul>
 *   <li><b>V1</b> — Original schema: {@code user_id}, {@code event_type}, {@code created_at}.</li>
 *   <li><b>V2</b> — Adds {@code session_id} and {@code ip_address} (both nullable).</li>
 *   <li><b>V3</b> — Widens {@code created_at} from INT to BIGINT (millisecond epoch);
 *       also adds {@code metadata} JSON blob (nullable). Detected when
 *       {@code created_at} overflows 32-bit range or {@code metadata} is present.</li>
 *   <li><b>UNKNOWN</b> — Fields don't match any known version; treated as V1 with
 *       nulls for missing fields.  Events landing here are also routed to the DLQ
 *       by {@link ai.streamforge.processor.deserialization.SchemaEvolutionFilter}.</li>
 * </ul>
 */
public enum SchemaVersion {
    V1,
    V2,
    V3,
    UNKNOWN
}
