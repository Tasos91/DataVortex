package com.thesis.datavortex.analytics.ml;

/**
 * Κλιμάκωση χαρακτηριστικών πριν το clustering.
 * <p>
 * Χωρίς αυτό, ένα πεδίο σε μονάδες 0–1000 (throughput) θα κυριαρχούσε πλήρως πάνω σε ένα
 * πεδίο 0–1 (ποσοστό σφαλμάτων), απλώς επειδή οι αριθμοί του είναι μεγαλύτεροι: η απόσταση
 * του k-means είναι ευκλείδεια και δεν ξέρει τίποτα για μονάδες μέτρησης.
 */
public enum Scaler {

    /** Μέσος 0, τυπική απόκλιση 1. Καλό όταν τα δεδομένα δεν έχουν ακραίες τιμές. */
    Z_SCORE,
    /** Διάμεσος 0, MAD 1. Το ίδιο, αλλά δεν παρασύρεται από ακραίες τιμές. */
    ROBUST;

    /**
     * @param data γραμμές = δείγματα, στήλες = χαρακτηριστικά (αλλάζει αντίγραφο, όχι το input)
     * @return κλιμακωμένος πίνακας· στήλη με μηδενική διασπορά γίνεται μηδενική
     */
    public double[][] fitTransform(double[][] data) {
        if (data.length == 0) return new double[0][0];
        int rows = data.length;
        int cols = data[0].length;
        double[][] out = new double[rows][cols];

        for (int c = 0; c < cols; c++) {
            double[] column = new double[rows];
            for (int r = 0; r < rows; r++) column[r] = data[r][c];

            double center = this == ROBUST ? Stats.median(column) : Stats.mean(column);
            double spread = this == ROBUST ? Stats.mad(column) : Stats.stddev(column);
            // Σταθερή στήλη: δεν προσφέρει πληροφορία, άρα μένει στο μηδέν αντί να γίνει NaN.
            boolean usable = Double.isFinite(spread) && spread > 1e-9;

            for (int r = 0; r < rows; r++) {
                out[r][c] = usable ? (column[r] - center) / spread : 0.0;
            }
        }
        return out;
    }
}
