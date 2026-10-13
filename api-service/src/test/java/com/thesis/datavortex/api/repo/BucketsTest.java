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
        // Ο στόχος είναι ~600 κουβάδες: λίγα λεπτά -> λεπτιαίο, μήνες -> ημερήσιο.
        assertEquals(Buckets.MINUTE, auto(Duration.ofMinutes(30)));
        assertEquals(Buckets.MINUTE, auto(Duration.ofHours(10)));   // 600 λεπτά, στο όριο
        assertEquals(Buckets.HOUR, auto(Duration.ofHours(11)));     // 660 λεπτά -> πολλά
        assertEquals(Buckets.HOUR, auto(Duration.ofDays(20)));
        assertEquals(Buckets.DAY, auto(Duration.ofDays(60)));
        assertEquals(Buckets.DAY, auto(Duration.ofDays(5000)));     // πάνω από 600 μέρες
    }

    @Test
    void explicitBucketWins() {
        assertEquals(Buckets.DAY, Buckets.of("1d", NOW.minusSeconds(60), NOW));
        assertEquals(Buckets.MINUTE, Buckets.of("1M", NOW.minus(Duration.ofDays(900)), NOW));
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
        // Το πλήθος εγγραφών υπάρχει μόνο σε λεπτιαίο· τα χονδρότερα αθροίζονται από αυτό.
        assertEquals("records_1m", Buckets.DAY.recordsSourceView());
    }
}
