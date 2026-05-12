package ai.streamforge.processor;

import ai.streamforge.processor.deserialization.SchemaAwareCdcDeserializationSchema;
import ai.streamforge.processor.deserialization.SchemaEvolutionFilter;
import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DeadLetterEvent;
import ai.streamforge.processor.model.UserEventCount;
import ai.streamforge.processor.serialization.DeadLetterEventSerializationSchema;
import ai.streamforge.processor.serialization.UserEventCountSerializationSchema;
import ai.streamforge.processor.sink.IcebergSinkFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Flink CDC aggregation job.
 *
 * Reads Debezium MySQL CDC events from Kafka, counts insert events per user
 * in tumbling event-time windows, and writes {@link UserEventCount} records
 * to an output Kafka topic.
 *
 * <p>Configuration via environment variables:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS}  — default {@code localhost:9092}</li>
 *   <li>{@code KAFKA_SOURCE_TOPIC}       — default {@code cdc.streamforge.user_events}</li>
 *   <li>{@code KAFKA_SINK_TOPIC}         — default {@code user.event.counts}</li>
 *   <li>{@code KAFKA_DLQ_TOPIC}          — default {@code cdc.dead.letter}; set empty to disable</li>
 *   <li>{@code KAFKA_CONSUMER_GROUP}     — default {@code flink-cdc-user-event-count}</li>
 *   <li>{@code WINDOW_SIZE_SECONDS}      — default {@code 60}</li>
 *   <li>{@code OUT_OF_ORDERNESS_SECONDS} — default {@code 5}</li>
 * </ul>
 *
 * <p>Flink parallelism and checkpointing knobs:
 * <ul>
 *   <li>{@code FLINK_PARALLELISM}              — operator parallelism, default {@code -1} (use cluster default)</li>
 *   <li>{@code CHECKPOINT_INTERVAL_MS}         — checkpoint interval in ms, default {@code 30000}</li>
 *   <li>{@code CHECKPOINT_TIMEOUT_MS}          — per-checkpoint timeout in ms, default {@code 60000}</li>
 *   <li>{@code CHECKPOINT_MIN_PAUSE_MS}        — min pause between checkpoints in ms, default {@code 0}</li>
 *   <li>{@code CHECKPOINT_MAX_CONCURRENT}      — max concurrent checkpoints, default {@code 1}</li>
 *   <li>{@code CHECKPOINT_MODE}                — {@code exactly_once} (default) or {@code at_least_once}</li>
 *   <li>{@code CHECKPOINT_UNALIGNED}           — enable unaligned checkpoints, default {@code false}</li>
 *   <li>{@code RESTART_ATTEMPTS}               — fixed-delay restart attempts, default {@code 3}</li>
 *   <li>{@code RESTART_DELAY_MS}               — fixed-delay restart interval in ms, default {@code 10000}</li>
 * </ul>
 *
 * <p>Kafka producer tuning knobs (applied to the main sink and DLQ sink):
 * <ul>
 *   <li>{@code KAFKA_SINK_COMPRESSION}         — {@code none} (default), {@code snappy}, {@code lz4}, {@code zstd}, {@code gzip}</li>
 *   <li>{@code KAFKA_SINK_BATCH_SIZE_BYTES}    — producer batch.size in bytes, default {@code 16384} (16 KB)</li>
 *   <li>{@code KAFKA_SINK_LINGER_MS}           — producer linger.ms, default {@code 5}</li>
 *   <li>{@code KAFKA_SINK_BUFFER_MEMORY_BYTES} — producer buffer.memory in bytes, default {@code 33554432} (32 MB)</li>
 *   <li>{@code KAFKA_SINK_ACKS}                — producer acks, default {@code all}</li>
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
        // ── Topic / group config ─────────────────────────────────────────────
        String bootstrapServers      = env("KAFKA_BOOTSTRAP_SERVERS",  "localhost:9092");
        String sourceTopic           = env("KAFKA_SOURCE_TOPIC",       "cdc.streamforge.user_events");
        String sinkTopic             = env("KAFKA_SINK_TOPIC",         "user.event.counts");
        String dlqTopic              = env("KAFKA_DLQ_TOPIC",          "cdc.dead.letter");
        String consumerGroup         = env("KAFKA_CONSUMER_GROUP",     "flink-cdc-user-event-count");
        long   windowSizeSeconds     = Long.parseLong(env("WINDOW_SIZE_SECONDS",      "60"));
        long   outOfOrdernessSeconds = Long.parseLong(env("OUT_OF_ORDERNESS_SECONDS", "5"));

        // ── Flink parallelism ────────────────────────────────────────────────
        int parallelism = Integer.parseInt(env("FLINK_PARALLELISM", "-1"));

        // ── Checkpoint config ────────────────────────────────────────────────
        long   checkpointIntervalMs   = Long.parseLong(env("CHECKPOINT_INTERVAL_MS",    "30000"));
        long   checkpointTimeoutMs    = Long.parseLong(env("CHECKPOINT_TIMEOUT_MS",     "60000"));
        long   checkpointMinPauseMs   = Long.parseLong(env("CHECKPOINT_MIN_PAUSE_MS",   "0"));
        int    maxConcurrentCkpts     = Integer.parseInt(env("CHECKPOINT_MAX_CONCURRENT", "1"));
        String checkpointModeStr      = env("CHECKPOINT_MODE",      "exactly_once");
        boolean unalignedCheckpoints  = Boolean.parseBoolean(env("CHECKPOINT_UNALIGNED", "false"));
        int    restartAttempts        = Integer.parseInt(env("RESTART_ATTEMPTS", "3"));
        long   restartDelayMs         = Long.parseLong(env("RESTART_DELAY_MS",  "10000"));

        // ── Kafka sink producer properties ───────────────────────────────────
        Properties kafkaSinkProps = buildKafkaSinkProps();

        LOG.info("Starting CdcUserEventCountJob: source={}, sink={}, dlq={}, window={}s, " +
                 "parallelism={}, checkpoint={}ms mode={} unaligned={}, " +
                 "compression={} batchSize={} lingerMs={}",
                sourceTopic, sinkTopic, dlqTopic.isBlank() ? "disabled" : dlqTopic, windowSizeSeconds,
                parallelism < 0 ? "cluster-default" : parallelism,
                checkpointIntervalMs, checkpointModeStr, unalignedCheckpoints,
                kafkaSinkProps.getProperty("compression.type", "none"),
                kafkaSinkProps.getProperty("batch.size", "16384"),
                kafkaSinkProps.getProperty("linger.ms", "5"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }

        // ── Checkpointing ────────────────────────────────────────────────────
        CheckpointingMode ckptMode = "at_least_once".equalsIgnoreCase(checkpointModeStr)
                ? CheckpointingMode.AT_LEAST_ONCE
                : CheckpointingMode.EXACTLY_ONCE;

        env.enableCheckpointing(checkpointIntervalMs, ckptMode);

        CheckpointConfig ckptCfg = env.getCheckpointConfig();
        ckptCfg.setCheckpointTimeout(checkpointTimeoutMs);
        ckptCfg.setMinPauseBetweenCheckpoints(checkpointMinPauseMs);
        ckptCfg.setMaxConcurrentCheckpoints(maxConcurrentCkpts);
        ckptCfg.enableUnalignedCheckpoints(unalignedCheckpoints);
        ckptCfg.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(restartAttempts, restartDelayMs));

        // ── Source ───────────────────────────────────────────────────────────
        KafkaSource<CdcEvent> source = KafkaSource.<CdcEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SchemaAwareCdcDeserializationSchema())
                .build();

        WatermarkStrategy<CdcEvent> watermarkStrategy = WatermarkStrategy
                .<CdcEvent>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessSeconds))
                .withTimestampAssigner((event, recordTimestamp) -> event.tsMs)
                .withIdleness(Duration.ofMinutes(1));

        // ── Schema evolution filter + dead-letter routing ────────────────────
        SingleOutputStreamOperator<CdcEvent> filteredEvents = env
                .fromSource(source, watermarkStrategy, "Kafka CDC Source")
                .process(new SchemaEvolutionFilter())
                .name("Schema Evolution Filter");

        DataStream<DeadLetterEvent> deadLetters =
                filteredEvents.getSideOutput(SchemaEvolutionFilter.DLQ_TAG);

        if (!dlqTopic.isBlank()) {
            KafkaSink<DeadLetterEvent> dlqSink = KafkaSink.<DeadLetterEvent>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setKafkaProducerConfig(kafkaSinkProps)
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.<DeadLetterEvent>builder()
                                    .setTopic(dlqTopic)
                                    .setValueSerializationSchema(new DeadLetterEventSerializationSchema())
                                    .build()
                    ).build();
            deadLetters.sinkTo(dlqSink).name("Kafka DLQ: " + dlqTopic);
        } else {
            deadLetters.print().name("DLQ Log");
        }

        // ── Main aggregation pipeline ────────────────────────────────────────
        DataStream<UserEventCount> counts = filteredEvents
                .filter(e -> "c".equals(e.op) && e.after != null && e.after.userId != null)
                .name("Filter: inserts only")
                .keyBy(e -> e.after.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSizeSeconds)))
                .aggregate(new EventCountAggregator(), new WindowMetadataFunction())
                .name("Aggregate: count events per user per window");

        KafkaSink<UserEventCount> sink = KafkaSink.<UserEventCount>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(kafkaSinkProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<UserEventCount>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new UserEventCountSerializationSchema())
                                .build()
                )
                .build();

        counts.sinkTo(sink).name("Kafka Sink: " + sinkTopic);

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

    // ── Aggregation functions ────────────────────────────────────────────────

    /** Incrementally accumulates a running event count. */
    static class EventCountAggregator implements AggregateFunction<CdcEvent, Long, Long> {
        @Override public Long createAccumulator()           { return 0L; }
        @Override public Long add(CdcEvent e, Long acc)     { return acc + 1; }
        @Override public Long getResult(Long acc)           { return acc; }
        @Override public Long merge(Long a, Long b)         { return a + b; }
    }

    /**
     * Attaches window boundaries and the keyed userId to the final count.
     * Also increments two Flink metrics:
     * <ul>
     *   <li>{@code windows_fired_total} — one per window that produces output</li>
     *   <li>{@code window_counts_emitted_total} — sum of all event counts across windows</li>
     * </ul>
     */
    static class WindowMetadataFunction
            extends RichProcessWindowFunction<Long, UserEventCount, String, TimeWindow> {

        private transient Counter windowsFired;
        private transient Counter countsEmitted;

        @Override
        public void open(Configuration parameters) {
            windowsFired   = getRuntimeContext().getMetricGroup().counter("windows_fired_total");
            countsEmitted  = getRuntimeContext().getMetricGroup().counter("window_counts_emitted_total");
        }

        @Override
        public void process(
                String userId,
                Context ctx,
                Iterable<Long> counts,
                Collector<UserEventCount> out) {
            long count = counts.iterator().next();
            windowsFired.inc();
            countsEmitted.inc(count);
            out.collect(new UserEventCount(userId, count, ctx.window().getStart(), ctx.window().getEnd()));
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String env(String name, String defaultValue) {
        String v = System.getenv(name);
        return (v != null && !v.isBlank()) ? v : defaultValue;
    }

    /**
     * Builds Kafka producer properties from environment variables.
     * These apply to both the main sink and the DLQ sink.
     */
    private static Properties buildKafkaSinkProps() {
        Properties props = new Properties();

        // Batching: larger batch + linger improves throughput at the cost of latency
        props.setProperty("batch.size",    env("KAFKA_SINK_BATCH_SIZE_BYTES",    "16384"));
        props.setProperty("linger.ms",     env("KAFKA_SINK_LINGER_MS",           "5"));
        props.setProperty("buffer.memory", env("KAFKA_SINK_BUFFER_MEMORY_BYTES", "33554432"));

        // Compression: reduces network/disk I/O; snappy/lz4 best for throughput, zstd for ratio
        String compression = env("KAFKA_SINK_COMPRESSION", "none");
        if (!"none".equalsIgnoreCase(compression)) {
            props.setProperty("compression.type", compression);
        }

        // Durability: "all" waits for all in-sync replicas; "1" or "0" for lower latency
        props.setProperty("acks", env("KAFKA_SINK_ACKS", "all"));

        return props;
    }
}
