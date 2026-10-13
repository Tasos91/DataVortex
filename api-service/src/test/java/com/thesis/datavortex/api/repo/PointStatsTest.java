package com.thesis.datavortex.api.repo;

import com.thesis.datavortex.api.model.Dtos;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Το μέσο και η τυπική απόκλιση βγαίνουν από sum και sum-of-squares, ώστε να μπορούν να
 * υπολογιστούν σε οποιοδήποτε επίπεδο συγκέντρωσης (λεπτό -> ώρα -> μέρα).
 */
class PointStatsTest {

    private static final Instant T = Instant.parse("2026-10-14T12:00:00Z");

    @Test
    void matchesTextbookStddev() {
        // Δείγμα: 2, 4, 4, 4, 5, 5, 7, 9 -> μέσος 5, δειγματική τυπική απόκλιση 2.138...
        double[] xs = {2, 4, 4, 4, 5, 5, 7, 9};
        double sum = 0, sumSq = 0;
        for (double x : xs) {
            sum += x;
            sumSq += x * x;
        }
        Dtos.Point p = AnalyticsRepository.point(T, xs.length, sum, sumSq, 2, 9);

        assertEquals(5.0, p.avg(), 1e-9);
        assertEquals(2.13809, p.stddev(), 1e-5);
        assertEquals(2.0, p.min());
        assertEquals(9.0, p.max());
        assertEquals(8, p.n());
    }

    @Test
    void rollsUpAcrossBuckets() {
        // Δύο κουβάδες που ενώνονται: το αποτέλεσμα πρέπει να είναι ίδιο με το ενιαίο δείγμα.
        double[] a = {1, 2, 3};
        double[] b = {10, 20};
        double sum = 0, sumSq = 0;
        for (double x : a) { sum += x; sumSq += x * x; }
        for (double x : b) { sum += x; sumSq += x * x; }

        Dtos.Point merged = AnalyticsRepository.point(T, 5, sum, sumSq, 1, 20);
        assertEquals(7.2, merged.avg(), 1e-9);
        assertEquals(7.981228, merged.stddev(), 1e-5);
    }

    @Test
    void singleSampleHasNoStddev() {
        Dtos.Point p = AnalyticsRepository.point(T, 1, 42.0, 42.0 * 42.0, 42, 42);
        assertEquals(42.0, p.avg());
        assertNull(p.stddev(), "με ένα δείγμα η δειγματική τυπική απόκλιση δεν ορίζεται");
    }

    @Test
    void emptyBucketHasNoAverage() {
        assertNull(AnalyticsRepository.point(T, 0, 0, 0, 0, 0).avg());
    }

    @Test
    void negativeVarianceFromRoundingBecomesZero() {
        // Με ίδιες τιμές, το sumSq - sum²/n μπορεί να βγει -1e-12 λόγω στρογγυλοποίησης.
        Dtos.Point p = AnalyticsRepository.point(T, 3, 3 * 0.1, 3 * 0.01, 0.1, 0.1);
        assertNotNull(p.stddev());
        assertTrue(p.stddev() >= 0.0, "η τυπική απόκλιση δεν μπορεί να είναι αρνητική");
    }
}
