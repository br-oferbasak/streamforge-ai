package ai.streamforge.processor.drift;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DriftMonitorFunctionTest {

    // ── computeDeltaRatio ────────────────────────────────────────────────────

    @Test
    void deltaRatio_firstWindow_isNaN() {
        assertTrue(Double.isNaN(DriftMonitorFunction.computeDeltaRatio(100, null)));
    }

    @Test
    void deltaRatio_prevZero_currentPositive_isPositiveInfinity() {
        assertEquals(Double.POSITIVE_INFINITY,
                DriftMonitorFunction.computeDeltaRatio(50, 0L));
    }

    @Test
    void deltaRatio_prevZero_currentZero_isZero() {
        assertEquals(0.0, DriftMonitorFunction.computeDeltaRatio(0, 0L));
    }

    @Test
    void deltaRatio_increase50Percent() {
        assertEquals(0.5, DriftMonitorFunction.computeDeltaRatio(150, 100L), 1e-9);
    }

    @Test
    void deltaRatio_decrease50Percent() {
        assertEquals(-0.5, DriftMonitorFunction.computeDeltaRatio(50, 100L), 1e-9);
    }

    @Test
    void deltaRatio_noChange_isZero() {
        assertEquals(0.0, DriftMonitorFunction.computeDeltaRatio(100, 100L), 1e-9);
    }

    // ── threshold boundary behaviour ─────────────────────────────────────────

    @Test
    void aboveThreshold_50pct_fires() {
        // 51% increase with threshold=0.50 should trigger
        double ratio = DriftMonitorFunction.computeDeltaRatio(151, 100L);
        assertTrue(Math.abs(ratio) > 0.50);
    }

    @Test
    void atThreshold_exactlyFiftyPct_doesNotFire() {
        // Exactly 50% — not strictly greater than threshold
        double ratio = DriftMonitorFunction.computeDeltaRatio(150, 100L);
        assertFalse(Math.abs(ratio) > 0.50);
    }
}
