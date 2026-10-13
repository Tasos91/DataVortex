package com.thesis.datavortex.analytics.ml;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ScalerTest {

    @Test
    void bigUnitsStopDominatingAfterScaling() {
        // Στήλη 0 σε χιλιάδες, στήλη 1 σε δεκαδικά: χωρίς κλιμάκωση η πρώτη κυριαρχεί πλήρως.
        double[][] data = {{1000, 0.1}, {2000, 0.2}, {3000, 0.3}};
        double[][] scaled = Scaler.Z_SCORE.fitTransform(data);

        for (int c = 0; c < 2; c++) {
            double[] column = {scaled[0][c], scaled[1][c], scaled[2][c]};
            assertEquals(0, Stats.mean(column), 1e-9, "μέσος 0 μετά την κλιμάκωση");
            assertEquals(1, Stats.stddev(column), 1e-9, "τυπική απόκλιση 1");
        }
        // Οι δύο στήλες έχουν πλέον το ίδιο εύρος, άρα το ίδιο βάρος στην απόσταση.
        assertEquals(scaled[0][0], scaled[0][1], 1e-9);
    }

    @Test
    void robustScalingIgnoresTheOutlier() {
        double[][] data = {{10}, {11}, {12}, {11}, {10}, {1000}};
        double[][] robust = Scaler.ROBUST.fitTransform(data);
        double[][] zscore = Scaler.Z_SCORE.fitTransform(data);

        // Με ανθεκτική κλιμάκωση τα κανονικά σημεία μένουν κοντά στο 0 και ο outlier εκτοξεύεται.
        assertTrue(Math.abs(robust[0][0]) < 2);
        assertTrue(robust[5][0] > 100);
        // Με z-score ο outlier "τραβά" τα πάντα και χάνεται η πληροφορία.
        assertTrue(zscore[5][0] < 3);
    }

    @Test
    void constantColumnBecomesZeroInsteadOfNaN() {
        double[][] data = {{5, 1}, {5, 2}, {5, 3}};
        for (Scaler scaler : Scaler.values()) {
            double[][] scaled = scaler.fitTransform(data);
            for (double[] row : scaled) {
                assertEquals(0.0, row[0], "σταθερή στήλη δεν προσφέρει πληροφορία");
                assertFalse(Double.isNaN(row[1]));
            }
        }
    }

    @Test
    void emptyInputIsSafe() {
        assertEquals(0, Scaler.ROBUST.fitTransform(new double[0][0]).length);
    }
}
