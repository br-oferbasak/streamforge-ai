package ai.streamforge.processor.model;

/**
 * Per-window drift observation emitted to the drift side topic.
 *
 * <p>Two signal types are detected:
 * <ul>
 *   <li><b>COUNT_DELTA</b> — the window's total event count deviated from the
 *       previous window by more than {@code DRIFT_COUNT_DELTA_THRESHOLD}.</li>
 *   <li><b>DISTRIBUTION_SKEW</b> — a single userId produced more than
 *       {@code DRIFT_SKEW_THRESHOLD} of all events in the window (hot-key).</li>
 * </ul>
 * A signal with type {@code BOTH} indicates both conditions fired simultaneously.
 */
public class DriftSignal {

    public long   windowStartMs;
    public long   windowEndMs;

    /** Total insert/snapshot events in the current window. */
    public long   totalEventCount;

    /** Total events in the immediately preceding window (−1 if no prior window). */
    public long   prevWindowEventCount;

    /**
     * {@code (current − prev) / prev}; {@code Double.NaN} for the first window,
     * {@code Double.POSITIVE_INFINITY} if prev was 0.
     */
    public double countDeltaRatio;

    /** userId with the highest event count in this window. */
    public String topUserId;

    /** Event count for {@link #topUserId}. */
    public long   topUserCount;

    /** {@code topUserCount / totalEventCount}; 0 if window is empty. */
    public double topUserSkewRatio;

    /** Which threshold(s) were crossed: COUNT_DELTA, DISTRIBUTION_SKEW, or BOTH. */
    public String triggeredBy;

    public DriftSignal() {}

    @Override
    public String toString() {
        return "DriftSignal{window=[" + windowStartMs + "," + windowEndMs
                + "], total=" + totalEventCount
                + ", deltaRatio=" + countDeltaRatio
                + ", topUser=" + topUserId + "(" + topUserSkewRatio + ")"
                + ", triggeredBy=" + triggeredBy + "}";
    }
}
