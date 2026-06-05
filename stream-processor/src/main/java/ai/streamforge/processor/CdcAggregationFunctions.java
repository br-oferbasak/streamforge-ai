package ai.streamforge.processor;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.UserEventCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Shared aggregation functions used by the live job and the backfill job. */
public final class CdcAggregationFunctions {

    private CdcAggregationFunctions() {}

    /** Incrementally accumulates a running event count. */
    public static class EventCountAggregator implements AggregateFunction<CdcEvent, Long, Long> {
        @Override public Long createAccumulator()           { return 0L; }
        @Override public Long add(CdcEvent e, Long acc)     { return acc + 1; }
        @Override public Long getResult(Long acc)           { return acc; }
        @Override public Long merge(Long a, Long b)         { return a + b; }
    }

    /**
     * Attaches window boundaries and the keyed userId to the final count.
     * Increments two Flink metrics per window:
     * <ul>
     *   <li>{@code windows_fired_total}</li>
     *   <li>{@code window_counts_emitted_total}</li>
     * </ul>
     */
    public static class WindowMetadataFunction
            extends RichProcessWindowFunction<Long, UserEventCount, String, TimeWindow> {

        private transient Counter windowsFired;
        private transient Counter countsEmitted;

        @Override
        public void open(Configuration parameters) {
            windowsFired  = getRuntimeContext().getMetricGroup().counter("windows_fired_total");
            countsEmitted = getRuntimeContext().getMetricGroup().counter("window_counts_emitted_total");
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
}
