package ai.streamforge.processor.backfill;

import ai.streamforge.processor.CdcAggregationFunctions.EventCountAggregator;
import ai.streamforge.processor.CdcAggregationFunctions.WindowMetadataFunction;
import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.DeadLetterEventSerializationSchema;
import ai.streamforge.processor.serialization.UserEventCountSerializationSchema;
import ai.streamforge.processor.sink.IcebergSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
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
 * Bounded Flink backfill job for replaying historical CDC events.
 *
 * <h2>Modes ({@code BACKFILL_MODE})</h2>
 * <dl>
 *   <dt>{@code kafka_range} (default)</dt>
 *   <dd>Replays a closed timestamp interval from the CDC source topic.
 *       Processes only {@code op=c} insert events, exactly mirroring the live job.
 *       Useful for recomputing aggregates after a sink failure or schema migration.</dd>
 *   <dt>{@code cdc_snapshot}</dt>
 *   <dd>Reads from the earliest offset to the latest offset at job-start time,
 *       accepting both {@code op=r} (Debezium initial-snapshot reads) and
 *       {@code op=c} inserts.  Use this to seed aggregates from scratch when
 *       the live job lagged behind an initial Debezium snapshot.</dd>
 * </dl>
 *
 * <h2>Configuration (environment variables)</h2>
 * Inherits all {@code KAFKA_*}, {@code WINDOW_SIZE_SECONDS},
 * {@code OUT_OF_ORDERNESS_SECONDS}, and {@code ICEBERG_*} variables from
 * {@link ai.streamforge.processor.CdcUserEventCountJob}, plus:
 * <ul>
 *   <li>{@code BACKFILL_MODE}              — {@code kafka_range} (default) or {@code cdc_snapshot}</li>
 *   <li>{@code BACKFILL_START_TIMESTAMP_MS} — inclusive start (epoch ms); kafka_range only</li>
 *   <li>{@code BACKFILL_END_TIMESTAMP_MS}   — inclusive end (epoch ms); kafka_range only;
 *       defaults to current wall-clock time at job startup</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}        — defaults to {@code flink-cdc-backfill}
 *       (intentionally different from the live job's group to avoid offset interference)</li>
 * </ul>
 *
 * <h2>Idempotency</h2>
 * See {@code docs/sink-idempotency.md} for the full contract.  In brief:
 * <ul>
 *   <li><b>Kafka sink</b> — at-least-once; downstream consumers must deduplicate on
 *       {@code (userId, windowStartMs, windowEndMs)}.</li>
 *   <li><b>Iceberg sink</b> — append mode; re-running backfill produces duplicate
 *       data files.  Run a post-backfill {@code DELETE} or use Iceberg's
 *       {@code MERGE INTO} to collapse duplicates before serving.</li>
 * </ul>
 */
public class BackfillJob {

    private static final Logger LOG = LoggerFactory.getLogger(BackfillJob.class);

    public static void main(String[] args) throws Exception {
        String mode                = env("BACKFILL_MODE",               "kafka_range");
        String bootstrapServers    = env("KAFKA_BOOTSTRAP_SERVERS",     "localhost:9092");
        String sourceTopic         = env("KAFKA_SOURCE_TOPIC",          "cdc.streamforge.user_events");
        String sinkTopic           = env("KAFKA_SINK_TOPIC",            "user.event.counts");
        String dlqTopic            = env("KAFKA_DLQ_TOPIC",             "cdc.dead.letter");
        // Dedicated consumer group so the backfill never advances the live job's offsets.
        String consumerGroup       = env("KAFKA_CONSUMER_GROUP",        "flink-cdc-backfill");
        long   windowSizeSeconds   = Long.parseLong(env("WINDOW_SIZE_SECONDS",       "60"));
        long   outOfOrderSecs      = Long.parseLong(env("OUT_OF_ORDERNESS_SECONDS",  "5"));

        BackfillMode backfillMode = BackfillMode.parse(mode);
        LOG.info("Starting BackfillJob: mode={}, source={}, sink={}, group={}",
                backfillMode, sourceTopic, sinkTopic, consumerGroup);

        KafkaSourceBuilder<CdcEvent> sourceBuilder = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setValueOnlyDeserializer(new SchemaAwareCdcDeserializationSchema());

        configureOffsets(sourceBuilder, backfillMode);

        KafkaSource<CdcEvent> source = sourceBuilder.build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        WatermarkStrategy<CdcEvent> watermarks = WatermarkStrategy
                .<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrderSecs))
                .withTimestampAssigner((event, ts) -> event.tsMs)
                .withIdleness(Duration.ofMinutes(1));

        SingleOutputStreamOperator<CdcEvent> filteredEvents = env
                .fromSource(source, watermarks, "Kafka CDC Source (backfill)")
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter");

        DataStream<DeadLetterEvent> deadLetters =
                filteredEvents.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        if (!dlqTopic.isBlank()) {
            KafkaSink<DeadLetterEvent> dlqSink = KafkaSink.<DeadLetterEvent>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.<DeadLetterEvent>builder()
                                    .setTopic(dlqTopic)
                                    .setValueSerializationSchema(new DeadLetterEventSerializationSchema())
                                    .build())
                    .build();
            deadLetters.sinkTo(dlqSink).name("Kafka DLQ: " + dlqTopic);
        } else {
            deadLetters.print().name("DLQ Log");
        }

        // cdc_snapshot mode also counts snapshot-read (op=r) rows so that
        // the initial Debezium snapshot contributes to the aggregated totals.
        DataStream<UserEventCount> counts = filteredEvents
                .filter(e -> acceptedOp(e, backfillMode) && e.after != null && e.after.userId != null)
                .name("Filter: " + opDescription(backfillMode))
                .keyBy(e -> e.after.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("Aggregate: count events per user per window (backfill)");

        KafkaSink<UserEventCount> sink = KafkaSink.<UserEventCount>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserEventCount>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new UserEventCountSerializationSchema())
                                .build())
                .build();

        counts.sinkTo(sink).name("Kafka Sink (backfill): " + sinkTopic);

        if (Boolean.parseBoolean(env("ICEBERG_ENABLED", "false"))) {
            String catalogType  = env("ICEBERG_CATALOG_TYPE",  "hadoop");
            String warehouse    = env("ICEBERG_WAREHOUSE",     "file:///tmp/iceberg-warehouse");
            String database     = env("ICEBERG_DATABASE",      "streamforge");
            String icebergTable = env("ICEBERG_TABLE",         "user_event_counts");
            String s3Endpoint   = env("ICEBERG_S3_ENDPOINT",   "");
            String s3AccessKey  = env("ICEBERG_S3_ACCESS_KEY", "");
            String s3SecretKey  = env("ICEBERG_S3_SECRET_KEY", "");
            String restUri      = env("ICEBERG_REST_URI",      "");

            LOG.info("Iceberg sink enabled (backfill): catalog={}, table={}.{}",
                    catalogType, database, icebergTable);
            IcebergSinkFactory.attach(counts, catalogType, warehouse, database, icebergTable,
                    s3Endpoint, s3AccessKey, s3SecretKey, restUri);
        }

        env.execute("BackfillJob[" + backfillMode + "]");
    }

    // ── Offset configuration ─────────────────────────────────────────────────

    static void configureOffsets(KafkaSourceBuilder<CdcEvent> builder, BackfillMode mode) {
        switch (mode) {
            case KAFKA_RANGE -> {
                long startMs = Long.parseLong(env("BACKFILL_START_TIMESTAMP_MS", "0"));
                long endMs   = Long.parseLong(env("BACKFILL_END_TIMESTAMP_MS",
                        String.valueOf(System.currentTimeMillis())));
                if (startMs <= 0) {
                    throw new IllegalArgumentException(
                            "BACKFILL_START_TIMESTAMP_MS must be set and > 0 for kafka_range mode");
                }
                LOG.info("Backfill range: startMs={}, endMs={}", startMs, endMs);
                builder.setStartingOffsets(OffsetsInitializer.timestamp(startMs))
                       .setBounded(OffsetsInitializer.timestamp(endMs));
            }
            case CDC_SNAPSHOT -> {
                // Bounded at the latest offset recorded at job startup; any events
                // produced after that point belong to the live pipeline.
                LOG.info("CDC snapshot mode: earliest → latest");
                builder.setStartingOffsets(OffsetsInitializer.earliest())
                       .setBounded(OffsetsInitializer.latest());
            }
        }
    }

    // ── Op-code filter helpers ───────────────────────────────────────────────

    /** Returns true if the event's op-code should be counted in this backfill mode. */
    static boolean acceptedOp(CdcEvent event, BackfillMode mode) {
        if ("c".equals(event.op)) return true;
        // cdc_snapshot also counts Debezium snapshot-read rows (op=r)
        return mode == BackfillMode.CDC_SNAPSHOT && "r".equals(event.op);
    }

    static String opDescription(BackfillMode mode) {
        return mode == BackfillMode.CDC_SNAPSHOT ? "inserts + snapshot reads" : "inserts only";
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }
}
