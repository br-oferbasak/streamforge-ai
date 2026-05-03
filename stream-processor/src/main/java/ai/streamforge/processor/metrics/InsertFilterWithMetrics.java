package ai.streamforge.processor.metrics;

import ai.streamforge.processor.model.CdcEvent;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * Replaces the inline insert-filter lambda in the job pipeline and exposes
 * three Flink metrics per task-manager instance:
 *
 * <ul>
 *   <li>{@code events_received_total}   — every event seen by this operator</li>
 *   <li>{@code insert_events_total}     — op=c events with a non-null userId that pass through</li>
 *   <li>{@code non_insert_events_total} — all other events (updates, deletes, snapshots)</li>
 *   <li>{@code insert_events_per_second} — 60-second sliding rate of inserts (Meter)</li>
 * </ul>
 *
 * <p>Metrics are scoped to the Flink operator metric group and can be scraped
 * via the Prometheus reporter — see {@code flink-metrics-example.properties}.
 */
public class InsertFilterWithMetrics extends RichFilterFunction<CdcEvent> {

    private transient Counter eventsReceived;
    private transient Counter insertsTotal;
    private transient Counter nonInsertsTotal;
    private transient Meter  insertsPerSecond;

    @Override
    public void open(Configuration parameters) {
        eventsReceived   = getRuntimeContext().getMetricGroup().counter("events_received_total");
        insertsTotal     = getRuntimeContext().getMetricGroup().counter("insert_events_total");
        nonInsertsTotal  = getRuntimeContext().getMetricGroup().counter("non_insert_events_total");
        insertsPerSecond = getRuntimeContext().getMetricGroup()
                .meter("insert_events_per_second", new MeterView(60));
    }

    @Override
    public boolean filter(CdcEvent event) {
        eventsReceived.inc();
        if (isValidInsert(event)) {
            insertsTotal.inc();
            insertsPerSecond.markEvent();
            return true;
        }
        nonInsertsTotal.inc();
        return false;
    }

    /** Extracted for unit-testability without a Flink runtime context. */
    static boolean isValidInsert(CdcEvent event) {
        return "c".equals(event.op) && event.after != null && event.after.userId != null;
    }
}
