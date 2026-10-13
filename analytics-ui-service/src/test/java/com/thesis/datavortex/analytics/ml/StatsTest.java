package com.thesis.datavortex.analytics.ml;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StatsTest {

    @Test
    void medianIgnoresOutliersThatWreckTheMean() {
        double[] withSpike = {10, 11, 12, 11, 10, 1000};
        assertEquals(11.0, Stats.median(withSpike), 1e-9);
        // Ο μέσος όρος παρασύρεται τελείως — γι' αυτό δεν τον χρησιμοποιούμε για ανωμαλίες.
        assertTrue(Stats.mean(withSpike) > 150);
    }

    @Test
    void madStaysSmallDespiteOneHugeValue() {
        double[] withSpike = {10, 11, 12, 11, 10, 1000};
        assertTrue(Stats.mad(withSpike) < 2.0, "το MAD δεν πρέπει να παρασυρθεί από μία τιμή");
        assertTrue(Stats.stddev(withSpike) > 300, "η τυπική απόκλιση παρασύρεται");
    }

    @Test
    void spikeIsFlaggedByRobustZ() {
        double[] xs = {10, 11, 12, 11, 10, 1000};
        double z = Stats.robustZ(1000, Stats.median(xs), Stats.mad(xs));
        assertTrue(z > 100, "μια τιμή 100x μακριά πρέπει να βγάζει τεράστιο z, βγήκε " + z);
    }

    @Test
    void constantDataProducesNoFalseAlarms() {
        double[] flat = {5, 5, 5, 5, 5};
        assertEquals(0, Stats.mad(flat), 1e-12);
        // Μηδενική διασπορά: κάθε z θα ήταν άπειρο, άρα επιστρέφουμε 0 αντί για συναγερμό.
        assertEquals(0, Stats.robustZ(5, 5, 0), 1e-12);
        assertEquals(0, Stats.robustZ(9, 5, 0), 1e-12);
    }

    @Test
    void quantilesInterpolate() {
        double[] xs = {1, 2, 3, 4};
        assertEquals(1.0, Stats.quantile(xs, 0));
        assertEquals(2.5, Stats.quantile(xs, 0.5), 1e-9);
        assertEquals(4.0, Stats.quantile(xs, 1));
        assertEquals(3.25, Stats.quantile(xs, 0.75), 1e-9);
    }

    @Test
    void ewmaFollowsTheLevelWithoutJumping() {
        double[] step = {0, 0, 0, 10, 10, 10, 10};
        double[] smoothed = Stats.ewma(step, 0.5);
        assertEquals(0, smoothed[2], 1e-9);
        assertEquals(5.0, smoothed[3], 1e-9);           // μισός δρόμος προς τη νέα στάθμη
        assertTrue(smoothed[6] > 9 && smoothed[6] < 10); // συγκλίνει χωρίς να φτάνει ακαριαία
    }

    @Test
    void trendDetectsDirectionAndIsScaleFree() {
        double[] rising = {10, 11, 12, 13, 14};
        double[] falling = {14, 13, 12, 11, 10};
        assertTrue(Stats.trendPerSample(rising) > 0);
        assertTrue(Stats.trendPerSample(falling) < 0);
        // Ίδιο σχετικό ανέβασμα σε άλλη κλίμακα: ίδια τιμή τάσης.
        double[] bigger = {100, 110, 120, 130, 140};
        assertEquals(Stats.trendPerSample(rising), Stats.trendPerSample(bigger), 1e-9);
    }

    @Test
    void emptyAndTinyInputsDoNotThrow() {
        assertTrue(Double.isNaN(Stats.median(new double[0])));
        assertEquals(0, Stats.stddev(new double[]{1}));
        assertEquals(0, Stats.trendPerSample(new double[]{1, 2}));
        assertEquals(0, Stats.ewma(new double[0], 0.5).length);
    }
}
