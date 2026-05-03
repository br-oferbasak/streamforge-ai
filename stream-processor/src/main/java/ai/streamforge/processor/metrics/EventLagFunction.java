package ai.streamforge.processor.metrics;

import ai.streamforge.processor.model.CdcEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.util.DescriptiveStatisticsHistogram;

/**
 * Pass-through operator that measures the lag between the Debezium event
 * timestamp ({@code ts_ms}) and the wall-clock time when Flink processes
 * the record, recording each sample in a histogram (milliseconds).
 *
 * <p>High p99 lag indicates the consumer is falling behind the CDC source.
 * Metric name: {@code event_processing_lag_ms}.
 *
 * <p>The histogram retains the last {@value #HISTOGRAM_SIZE} samples; this
 * is large enough to give stable percentiles without unbounded memory use.
 */
public class EventLagFunction extends RichMapFunction<CdcEvent, CdcEvent> {

    private static final int HISTOGRAM_SIZE = 1024;

    private transient Histogram eventLagMs;

    @Override
    public void open(Configuration parameters) {
        eventLagMs = getRuntimeContext().getMetricGroup()
                .histogram("event_processing_lag_ms", new DescriptiveStatisticsHistogram(HISTOGRAM_SIZE));
    }

    @Override
    public CdcEvent map(CdcEvent event) {
        long lag = System.currentTimeMillis() - event.tsMs;
        if (lag >= 0) {
            eventLagMs.update(lag);
        }
        return event;
    }
}
