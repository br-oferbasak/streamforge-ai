package ai.streamforge.processor;

import ai.streamforge.processor.CdcAggregationFunctions.EventCountAggregator;
import ai.streamforge.processor.CdcAggregationFunctions.WindowMetadataFunction;
import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.drift.DriftMonitorFunction;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.DriftSignal;
import ai.streamforge.processor.model.QuarantineEvent;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.DeadLetterEventSerializationSchema;
import ai.streamforge.processor.serialization.DriftSignalSerializationSchema;
import ai.streamforge.processor.serialization.QuarantineEventSerializationSchema;
import ai.streamforge.processor.serialization.UserEventCountSerializationSchema;
import ai.streamforge.processor.sink.IcebergSinkFactory;
import ai.streamforge.processor.validation.CdcEventValidator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink CDC aggregation job.
 *
 * Reads Debezium MySQL CDC events from Kafka, counts insert events per user
 * in tumbling event-time windows, and writes {@link UserEventCount} records
 * to an output Kafka topic.
 *
 * <h2>Pipeline stages</h2>
 * <ol>
 *   <li><b>Schema evolution filter</b> — routes unresolvable records to the DLQ topic.</li>
 *   <li><b>Inline validator</b> — checks schema, nulls, and value ranges; quarantines
 *       bad rows to the quarantine topic.</li>
 *   <li><b>Drift monitor</b> — side pipeline keyed by constant "global"; detects
 *       row-count deltas and per-user distribution skew; emits signals to the drift topic.</li>
 *   <li><b>Main aggregation</b> — counts inserts per user per tumbling window;
 *       writes to the sink topic and optionally Iceberg.</li>
 * </ol>
 *
 * <h2>Configuration (environment variables)</h2>
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — default {@code cdc.streamforge.user_events}</li>
 *   <li>{@code KAFKA_SINK_TOPIC}         — default {@code user.event.counts}</li>
 *   <li>{@code KAFKA_DLQ_TOPIC}          — default {@code cdc.dead.letter}; blank = disabled</li>
 *   <li>{@code KAFKA_QUARANTINE_TOPIC}   — default {@code cdc.quarantine}</li>
 *   <li>{@code KAFKA_DRIFT_TOPIC}        — default {@code cdc.drift.signals}</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-cdc-user-event-count}</li>
 *   <li>{@code WINDOW_SIZE_SECONDS}      — default {@code 60}</li>
 *   <li>{@code OUT_OF_ORDERNESS_SECONDS} — default {@code 5}</li>
 *   <li>{@code VALIDATION_MAX_FUTURE_SKEW_MS} — default {@code 3600000} (1 h)</li>
 *   <li>{@code VALIDATION_USER_ID_MAX_LEN}    — default {@code 128}</li>
 *   <li>{@code DRIFT_COUNT_DELTA_THRESHOLD}   — default {@code 0.5}</li>
 *   <li>{@code DRIFT_SKEW_THRESHOLD}          — default {@code 0.3}</li>
 *   <li>{@code DRIFT_MIN_WINDOW_COUNT}        — default {@code 10}</li>
 * </ul>
 *
 * <p>Optional Apache Iceberg sink (set {@code ICEBERG_ENABLED=true} to activate):
 * <ul>
 *   <li>{@code ICEBERG_ENABLED}       — default {@code false}</li>
 *   <li>{@code ICEBERG_CATALOG_TYPE}  — {@code hadoop} (default), {@code hive}, or {@code rest}</li>
 *   <li>{@code ICEBERG_WAREHOUSE}     — default {@code file:///tmp/iceberg-warehouse}</li>
 *   <li>{@code ICEBERG_DATABASE}      — default {@code streamforge}</li>
 *   <li>{@code ICEBERG_TABLE}         — default {@code user_event_counts}</li>
 *   <li>{@code ICEBERG_S3_ENDPOINT}   — S3/MinIO endpoint, e.g. {@code http://minio:9000}</li>
 *   <li>{@code ICEBERG_S3_ACCESS_KEY} — S3/MinIO access key</li>
 *   <li>{@code ICEBERG_S3_SECRET_KEY} — S3/MinIO secret key</li>
 * </ul>
 */
public class CdcUserEventCountJob {

    private static final Logger LOG = LoggerFactory.getLogger(CdcUserEventCountJob.class);

    public static void main(String[] args) throws Exception {
        String bootstrapServers   = env("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092");
        String sourceTopic        = env("KAFKA_SOURCE_TOPIC",       "cdc.streamforge.user_events");
        String sinkTopic          = env("KAFKA_SINK_TOPIC",         "user.event.counts");
        String dlqTopic           = env("KAFKA_DLQ_TOPIC",          "cdc.dead.letter");
        String quarantineTopic    = env("KAFKA_QUARANTINE_TOPIC",   "cdc.quarantine");
        String driftTopic         = env("KAFKA_DRIFT_TOPIC",        "cdc.drift.signals");
        String consumerGroup      = env("KAFKA_CONSUMER_GROUP",     "flink-cdc-user-event-count");
        long   windowSizeSeconds  = Long.parseLong(env("WINDOW_SIZE_SECONDS",      "60"));
        long   outOfOrderSecs     = Long.parseLong(env("OUT_OF_ORDERNESS_SECONDS", "5"));

        long   maxFutureSkewMs    = Long.parseLong(env("VALIDATION_MAX_FUTURE_SKEW_MS", "3600000"));
        int    userIdMaxLen       = Integer.parseInt(env("VALIDATION_USER_ID_MAX_LEN",  "128"));
        double deltaThreshold     = Double.parseDouble(env("DRIFT_COUNT_DELTA_THRESHOLD", "0.5"));
        double skewThreshold      = Double.parseDouble(env("DRIFT_SKEW_THRESHOLD",        "0.3"));
        long   minWindowCount     = Long.parseLong(env("DRIFT_MIN_WINDOW_COUNT",          "10"));

        LOG.info("Starting CdcUserEventCountJob: source={}, sink={}, quarantine={}, drift={}, window={}s",
                sourceTopic, sinkTopic, quarantineTopic, driftTopic, windowSizeSeconds);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<CdcEvent> source = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SchemaAwareCdcDeserializationSchema())
                .build();

        WatermarkStrategy<CdcEvent> watermarks = WatermarkStrategy
                .<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSecs))
                .withTimestampAssigner((event, ts) -> event.tsMs)
                .withIdleness(Duration.ofMinutes(1));

        // ── Stage 1: Schema evolution filter → DLQ ───────────────────────────
        SingleOutputStreamOperator<CdcEvent> schemaFiltered = env
                .fromSource(source, watermarks, "Kafka CDC Source")
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter");

        DataStream<DeadLetterEvent> deadLetters =
                schemaFiltered.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        if (!dlqTopic.isBlank()) {
            deadLetters.sinkTo(kafkaSink(bootstrapServers, dlqTopic,
                    new DeadLetterEventSerializationSchema()))
                    .name("Kafka DLQ: " + dlqTopic);
        } else {
            deadLetters.print().name("DLQ Log");
        }

        // ── Stage 2: Inline validator → quarantine ───────────────────────────
        SingleOutputStreamOperator<CdcEvent> validatedEvents = schemaFiltered
                .process(new CdcEventValidator(maxFutureSkewMs, userIdMaxLen))
                .name("Inline Validator");

        DataStream<QuarantineEvent> quarantined =
                validatedEvents.getSideOutput(CdcEventValidator.QUARANTINE_TAG);

        quarantined.sinkTo(kafkaSink(bootstrapServers, quarantineTopic,
                new QuarantineEventSerializationSchema()))
                .name("Kafka Quarantine: " + quarantineTopic);

        // ── Stage 3: Drift monitor (side pipeline) ───────────────────────────
        // Key by constant so all events within a window reach a single task,
        // enabling cross-user skew detection without a shuffle on the hot path.
        DataStream<DriftSignal> driftSignals = validatedEvents
                .filter(e -> "c".equals(e.op) || "r".equals(e.op))
                .name("Drift filter: inserts + reads")
                .keyBy(e -> "global")
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .process(new DriftMonitorFunction(deltaThreshold, skewThreshold, minWindowCount))
                .name("Drift Monitor");

        driftSignals.sinkTo(kafkaSink(bootstrapServers, driftTopic,
                new DriftSignalSerializationSchema()))
                .name("Kafka Drift: " + driftTopic);

        // ── Stage 4: Main aggregation pipeline ───────────────────────────────
        DataStream<UserEventCount> counts = validatedEvents
                .filter(e -> "c".equals(e.op) && e.after != null && e.after.userId != null)
                .name("Filter: inserts only")
                .keyBy(e -> e.after.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("Aggregate: count events per user per window");

        counts.sinkTo(kafkaSink(bootstrapServers, sinkTopic,
                new UserEventCountSerializationSchema()))
                .name("Kafka Sink: " + sinkTopic);

        // ── Optional Iceberg sink ────────────────────────────────────────────
        if (Boolean.parseBoolean(env("ICEBERG_ENABLED", "false"))) {
            String catalogType  = env("ICEBERG_CATALOG_TYPE",  "hadoop");
            String warehouse    = env("ICEBERG_WAREHOUSE",     "file:///tmp/iceberg-warehouse");
            String database     = env("ICEBERG_DATABASE",      "streamforge");
            String icebergTable = env("ICEBERG_TABLE",         "user_event_counts");
            String s3Endpoint   = env("ICEBERG_S3_ENDPOINT",   "");
            String s3AccessKey  = env("ICEBERG_S3_ACCESS_KEY", "");
            String s3SecretKey  = env("ICEBERG_S3_SECRET_KEY", "");

            LOG.info("Iceberg sink enabled: catalog={}, warehouse={}, table={}.{}",
                    catalogType, warehouse, database, icebergTable);
            IcebergSinkFactory.attach(counts, catalogType, warehouse, database, icebergTable,
                    s3Endpoint, s3AccessKey, s3SecretKey);
        }

        env.execute("CdcUserEventCountJob");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static <T> KafkaSink<T> kafkaSink(
            String bootstrapServers,
            String topic,
            org.apache.flink.api.common.serialization.SerializationSchema<T> schema) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(schema)
                                .build())
                .build();
    }

    static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
