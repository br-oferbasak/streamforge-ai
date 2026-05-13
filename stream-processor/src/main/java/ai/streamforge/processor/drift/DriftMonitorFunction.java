package ai.streamforge.processor.drift;

import ai.streamforge.processor.model.CdcEvent;
import ai.streamforge.processor.model.DriftSignal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Computes per-window drift signals from the validated event stream.
 *
 * <p>Designed to be applied on a stream keyed by the constant {@code "global"}
 * so that all events in a window are collected on a single task, enabling
 * cross-user aggregation without an expensive shuffle.  This is an
 * observability side-channel — throughput is expected to be much lower than
 * the main aggregation path.
 *
 * <h3>Signals emitted</h3>
 * <ul>
 *   <li><b>COUNT_DELTA</b> — {@code |current − prev| / prev} exceeds
 *       {@code countDeltaThreshold} and the window has at least
 *       {@code minWindowCount} events.</li>
 *   <li><b>DISTRIBUTION_SKEW</b> — a single userId accounts for more than
 *       {@code skewThreshold} of all events in the window.</li>
 *   <li><b>BOTH</b> — both conditions fire in the same window.</li>
 * </ul>
 *
 * <p>Windows below {@code minWindowCount} are silently skipped to avoid
 * noisy signals during startup or low-traffic periods.
 *
 * <h3>Configuration (env vars read by the job)</h3>
 * <ul>
 *   <li>{@code DRIFT_COUNT_DELTA_THRESHOLD} — default {@code 0.5} (50 %)</li>
 *   <li>{@code DRIFT_SKEW_THRESHOLD}        — default {@code 0.3} (30 %)</li>
 *   <li>{@code DRIFT_MIN_WINDOW_COUNT}      — default {@code 10}</li>
 * </ul>
 */
public class DriftMonitorFunction
        extends RichProcessWindowFunction<CdcEvent, DriftSignal, String, TimeWindow> {

    private final double countDeltaThreshold;
    private final double skewThreshold;
    private final long   minWindowCount;

    /** Previous window's total event count; persisted in Flink keyed state. */
    private transient ValueState<Long> prevCountState;

    public DriftMonitorFunction(
            double countDeltaThreshold,
            double skewThreshold,
            long   minWindowCount) {
        this.countDeltaThreshold = countDeltaThreshold;
        this.skewThreshold       = skewThreshold;
        this.minWindowCount      = minWindowCount;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        prevCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("drift-prev-window-count", Long.class));
    }

    @Override
    public void process(
            String key,
            Context ctx,
            Iterable<CdcEvent> events,
            Collector<DriftSignal> out) throws Exception {

        // ── 1. Aggregate: total count + per-user histogram ───────────────────
        Map<String, Long> userCounts = new HashMap<>();
        long total = 0;
        for (CdcEvent e : events) {
            total++;
            if (e.after != null && e.after.userId != null) {
                userCounts.merge(e.after.userId, 1L, Long::sum);
            }
        }

        if (total < minWindowCount) {
            prevCountState.update(total);
            return;
        }

        // ── 2. Top user (skew detection) ─────────────────────────────────────
        String topUserId    = null;
        long   topUserCount = 0;
        for (Map.Entry<String, Long> entry : userCounts.entrySet()) {
            if (entry.getValue() > topUserCount) {
                topUserCount = entry.getValue();
                topUserId    = entry.getKey();
            }
        }

        double skewRatio = total > 0 ? (double) topUserCount / total : 0.0;

        // ── 3. Count delta ───────────────────────────────────────────────────
        Long   prevCount  = prevCountState.value();
        double deltaRatio = computeDeltaRatio(total, prevCount);

        prevCountState.update(total);

        // ── 4. Threshold checks — only emit when at least one fires ──────────
        boolean countDeltaFired = prevCount != null
                && Math.abs(deltaRatio) > countDeltaThreshold;
        boolean skewFired       = skewRatio > skewThreshold;

        if (!countDeltaFired && !skewFired) {
            return;
        }

        // ── 5. Build and emit signal ─────────────────────────────────────────
        DriftSignal signal = new DriftSignal();
        signal.windowStartMs        = ctx.window().getStart();
        signal.windowEndMs          = ctx.window().getEnd();
        signal.totalEventCount      = total;
        signal.prevWindowEventCount = prevCount != null ? prevCount : -1L;
        signal.countDeltaRatio      = deltaRatio;
        signal.topUserId            = topUserId;
        signal.topUserCount         = topUserCount;
        signal.topUserSkewRatio     = skewRatio;
        signal.triggeredBy          = triggerLabel(countDeltaFired, skewFired);

        out.collect(signal);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    static double computeDeltaRatio(long current, Long prev) {
        if (prev == null) return Double.NaN;
        if (prev == 0)    return current > 0 ? Double.POSITIVE_INFINITY : 0.0;
        return (double) (current - prev) / prev;
    }

    private static String triggerLabel(boolean countDelta, boolean skew) {
        if (countDelta && skew) return "BOTH";
        if (countDelta)         return "COUNT_DELTA";
        return "DISTRIBUTION_SKEW";
    }
}
