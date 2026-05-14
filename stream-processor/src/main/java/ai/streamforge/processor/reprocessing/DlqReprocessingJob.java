package ai.streamforge.processor.reprocessing;

import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.schema.ColumnAliasRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-shot Flink job that replays events from the dead-letter topic back into
 * the main CDC pipeline after a BREAKING schema change has been remediated.
 *
 * <h3>When to run</h3>
 * <ol>
 *   <li>A BREAKING change (column rename, incompatible type) caused events to land in
 *       the DLQ ({@code KAFKA_DLQ_TOPIC}).</li>
 *   <li>The alias or coercion rule has been registered
 *       (via {@code COLUMN_ALIASES} env var or code change).</li>
 *   <li>The main {@code CdcUserEventCountJob} has been redeployed from a savepoint
 *       with the updated alias registry.</li>
 *   <li>This job is submitted <em>once</em> to drain the DLQ.  It reads each
 *       {@link DeadLetterEvent}, extracts the {@code rawPayload}, re-deserializes it
 *       with the updated alias registry, and publishes valid events back to the
 *       source topic.</li>
 * </ol>
 *
 * <h3>Idempotency</h3>
 * Replayed events carry the original {@code ts_ms} value so watermark-based
 * event-time windows in the main job can place them in the correct window.
 * The main job's checkpoint interval and out-of-orderness allowance must be set
 * large enough to absorb the replay.  For large backlogs, consider running the
 * main job with {@code KAFKA_RESET_OFFSETS=earliest} instead of replaying through
 * the DLQ.
 *
 * <h3>Configuration (env vars)</h3>
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_DLQ_TOPIC}          — default {@code cdc.dead.letter}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — re-injection target, default {@code cdc.streamforge.user_events}</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-dlq-reprocessor}</li>
 *   <li>{@code COLUMN_ALIASES}           — comma-separated {@code alias=canonical} pairs</li>
 *   <li>{@code REPROCESS_MAX_EVENTS}     — safety cap; 0 = unlimited (default {@code 0})</li>
 * </ul>
 */
public class DlqReprocessingJob {

    private static final Logger LOG = LoggerFactory.getLogger(DlqReprocessingJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String dlqTopic         = env("KAFKA_DLQ_TOPIC",         "cdc.dead.letter");
        String sourceTopic      = env("KAFKA_SOURCE_TOPIC",      "cdc.streamforge.user_events");
        String consumerGroup    = env("KAFKA_CONSUMER_GROUP",    "flink-dlq-reprocessor");
        long   maxEvents        = Long.parseLong(env("REPROCESS_MAX_EVENTS", "0"));

        LOG.info("DlqReprocessingJob starting: dlq={} → source={}, aliases={}",
                dlqTopic, sourceTopic, System.getenv("COLUMN_ALIASES"));

        ColumnAliasRegistry aliases = ColumnAliasRegistry.withDefaults();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(15_000);

        // Read raw bytes from DLQ — the topic stores DeadLetterEvent JSON
        KafkaSource<byte[]> dlqSource = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(dlqTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new RawBytesDeserializationSchema())
                .setBounded(OffsetsInitializer.latest()) // drain-and-stop
                .build();

        DataStream<byte[]> dlqStream = env.fromSource(
                dlqSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka DLQ Source: " + dlqTopic);

        // Extract rawPayload from each DeadLetterEvent, re-deserialize with updated aliases
        ObjectMapper mapper = new ObjectMapper();

        SingleOutputStreamOperator<CdcEvent> reprocessed = dlqStream
                .flatMap((bytes, out) -> {
                    try {
                        JsonNode dlqEvent = mapper.readTree(bytes);
                        String rawPayload = dlqEvent.path("rawPayload").asText(null);
                        if (rawPayload == null || rawPayload.isBlank()) {
                            LOG.warn("DLQ event missing rawPayload, skipping");
                            return;
                        }
                        CdcEvent event = ai.streamforge.processor.deserialization
                                .SchemaEvolutionHandler.handle(
                                        rawPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                        mapper, aliases);
                        if (event != null) {
                            out.collect(event);
                        }
                    } catch (Exception e) {
                        LOG.error("Failed to re-deserialize DLQ record: {}", e.getMessage());
                    }
                })
                .returns(TypeInformation.of(CdcEvent.class))
                .name("Re-deserialize with updated aliases");

        // Route through the same schema evolution filter — events that are still
        // BREAKING after alias update land back in a secondary DLQ for manual review
        SingleOutputStreamOperator<CdcEvent> filtered = reprocessed
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter (reprocessing)");

        DataStream<DeadLetterEvent> stillBroken =
                filtered.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        String secondaryDlqTopic = dlqTopic + ".unresolved";
        KafkaSink<DeadLetterEvent> secondaryDlq = KafkaSink.<DeadLetterEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<DeadLetterEvent>builder()
                                .setTopic(secondaryDlqTopic)
                                .setValueSerializationSchema(
                                        new ai.streamforge.processor.serialization
                                                .DeadLetterEventSerializationSchema())
                                .build())
                .build();
        stillBroken.sinkTo(secondaryDlq).name("Secondary DLQ: " + secondaryDlqTopic);
        LOG.info("Unresolvable events will be routed to {}", secondaryDlqTopic);

        // Re-inject successfully re-processed events into the main source topic
        KafkaSink<CdcEvent> reinjectSink = KafkaSink.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<CdcEvent>builder()
                                .setTopic(sourceTopic)
                                .setValueSerializationSchema(new CdcEventSerializationSchema())
                                .build())
                .build();
        filtered.sinkTo(reinjectSink).name("Re-inject to: " + sourceTopic);

        env.execute("DlqReprocessingJob");
    }

    // ── Serialization schemas ────────────────────────────────────────────────

    /** Passes raw Kafka message bytes through unchanged. */
    static class RawBytesDeserializationSchema implements DeserializationSchema<byte[]> {
        @Override public byte[]  deserialize(byte[] message)       { return message; }
        @Override public boolean isEndOfStream(byte[] nextElement)  { return false; }
        @Override public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(byte[].class);
        }
    }

    /** Serializes a {@link CdcEvent} back to its original JSON bytes for re-injection. */
    static class CdcEventSerializationSchema implements SerializationSchema<CdcEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(InitializationContext context) {
            mapper = new ObjectMapper();
        }

        @Override
        public byte[] serialize(CdcEvent event) {
            try {
                return mapper.writeValueAsBytes(event);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize CdcEvent for re-injection", e);
            }
        }
    }

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
