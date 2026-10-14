package com.thesis.datavortex.api.repo;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class BucketsTest {

    private static final Instant NOW = Instant.parse("2026-10-14T12:00:00Z");

    private static Buckets auto(Duration range) {
        return Buckets.of("auto", NOW.minus(range), NOW);
    }

    @Test
    void autoKeepsAnswersSmallWhateverTheRange() {
        // Στόχος ~800 κουβάδες: λίγες ώρες -> λεπτιαίο, μήνες -> ημερήσιο.
        assertEquals(Buckets.MINUTE, auto(Duration.ofMinutes(30)));
        assertEquals(Buckets.MINUTE, auto(Duration.ofHours(13)));        // 780 λεπτά
        assertEquals(Buckets.FIVE_MINUTES, auto(Duration.ofHours(14)));  // 840 -> πολλά
        assertEquals(Buckets.FIVE_MINUTES, auto(Duration.ofDays(2)));
        assertEquals(Buckets.QUARTER_HOUR, auto(Duration.ofDays(5)));
        assertEquals(Buckets.HOUR, auto(Duration.ofDays(20)));
        assertEquals(Buckets.SIX_HOURS, auto(Duration.ofDays(60)));
        assertEquals(Buckets.DAY, auto(Duration.ofDays(5000)));
    }

    @Test
    void theLadderHasNoBigJumps() {
        // Χωρίς ενδιάμεσα σκαλοπάτια, ένα παράθυρο 14 ωρών έπεφτε από 780 κουβάδες σε 14:
        // το διάγραμμα γινόταν άχρηστο και η ομαδοποίηση έβρισκε ομάδες από ένα σημείο.
        Buckets[] ladder = Buckets.values();
        for (int i = 1; i < ladder.length; i++) {
            double ratio = (double) ladder[i].width().getSeconds() / ladder[i - 1].width().getSeconds();
            assertTrue(ratio <= 6.0,
                    "άλμα x%.0f από %s σε %s".formatted(ratio, ladder[i - 1].id(), ladder[i].id()));
        }
    }

    @Test
    void everyRangeKeepsEnoughPointsToAnalyse() {
        // Σε κάθε ρεαλιστικό παράθυρο πρέπει να μένουν αρκετοί κουβάδες για ομαδοποίηση.
        for (Duration range : new Duration[]{Duration.ofHours(1), Duration.ofHours(6), Duration.ofHours(12),
                Duration.ofDays(1), Duration.ofDays(7), Duration.ofDays(30)}) {
            Buckets bucket = auto(range);
            long buckets = range.getSeconds() / bucket.width().getSeconds();
            assertTrue(buckets >= 60 && buckets <= 800,
                    "%s -> %s δίνει %d κουβάδες".formatted(range, bucket.id(), buckets));
        }
    }

    @Test
    void explicitBucketWins() {
        assertEquals(Buckets.DAY, Buckets.of("1d", NOW.minusSeconds(60), NOW));
        assertEquals(Buckets.MINUTE, Buckets.of("1M", NOW.minus(Duration.ofDays(900)), NOW));
        assertEquals(Buckets.FIVE_MINUTES, Buckets.of("5m", NOW.minusSeconds(60), NOW));
        assertEquals(Buckets.SIX_HOURS, Buckets.of("6h", NOW.minusSeconds(60), NOW));
    }

    @Test
    void emptyOrBrokenRangeDoesNotThrow() {
        assertEquals(Buckets.MINUTE, Buckets.of(null, NOW, NOW));
        assertEquals(Buckets.MINUTE, Buckets.of("", NOW, NOW.minusSeconds(10)));
    }

    @Test
    void unknownBucketIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Buckets.of("5s", NOW.minusSeconds(60), NOW));
    }

    @Test
    void eachBucketKnowsItsView() {
        assertEquals("metric_1m", Buckets.MINUTE.metricView());
        assertEquals("metric_1h", Buckets.HOUR.metricView());
        assertEquals("metric_1d", Buckets.DAY.metricView());
        // Τα ενδιάμεσα επίπεδα διαβάζουν από το αμέσως λεπτομερέστερο συγκεντρωτικό.
        assertEquals("metric_1m", Buckets.FIVE_MINUTES.metricView());
        assertEquals("metric_1m", Buckets.QUARTER_HOUR.metricView());
        assertEquals("metric_1h", Buckets.SIX_HOURS.metricView());
    }
}
