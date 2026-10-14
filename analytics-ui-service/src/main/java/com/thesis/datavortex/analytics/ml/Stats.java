package com.thesis.datavortex.analytics.ml;

import java.util.Arrays;

/**
 * Στατιστικά που αντέχουν σε ακραίες τιμές.
 * <p>
 * Ο μέσος όρος και η τυπική απόκλιση είναι άχρηστοι για ανίχνευση ανωμαλιών: μία ακραία τιμή
 * τους μετακινεί και κρύβει τον εαυτό της. Η διάμεσος και το MAD (median absolute deviation)
 * δεν επηρεάζονται, γι' αυτό χρησιμοποιούνται εδώ.
 */
public final class Stats {

    /** Μετατρέπει το MAD σε εκτίμηση τυπικής απόκλισης για κανονική κατανομή. */
    private static final double MAD_TO_SIGMA = 1.4826;

    private Stats() {}

    /**
     * Κρατά μόνο τις υπαρκτές τιμές.
     * <p>
     * Ένα πεδίο μπορεί να λείπει από κάποιους κουβάδες (δεν μετρήθηκε). Αν το μετρήσουμε ως
     * μηδέν, η διάμεσος πέφτει, η διασπορά φουσκώνει, και το ίδιο το "κενό" εμφανίζεται ως
     * ανωμαλία — πράγμα που δεν συνέβη ποτέ.
     */
    public static double[] present(double[] xs) {
        int count = 0;
        for (double x : xs) {
            if (Double.isFinite(x)) count++;
        }
        if (count == xs.length) return xs;
        double[] out = new double[count];
        int i = 0;
        for (double x : xs) {
            if (Double.isFinite(x)) out[i++] = x;
        }
        return out;
    }

    public static double mean(double[] raw) {
        double[] xs = present(raw);
        if (xs.length == 0) return Double.NaN;
        double sum = 0;
        for (double x : xs) sum += x;
        return sum / xs.length;
    }

    public static double stddev(double[] raw) {
        double[] xs = present(raw);
        if (xs.length < 2) return 0;
        double m = mean(xs);
        double sum = 0;
        for (double x : xs) sum += (x - m) * (x - m);
        return Math.sqrt(sum / (xs.length - 1));
    }

    public static double median(double[] raw) {
        double[] xs = present(raw);
        if (xs.length == 0) return Double.NaN;
        double[] copy = xs.clone();
        Arrays.sort(copy);
        int mid = copy.length / 2;
        return copy.length % 2 == 1 ? copy[mid] : (copy[mid - 1] + copy[mid]) / 2.0;
    }

    /** Ποσοστημόριο με γραμμική παρεμβολή (0.0–1.0). */
    public static double quantile(double[] raw, double q) {
        double[] xs = present(raw);
        if (xs.length == 0) return Double.NaN;
        double[] copy = xs.clone();
        Arrays.sort(copy);
        double pos = Math.max(0, Math.min(1, q)) * (copy.length - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        return lo == hi ? copy[lo] : copy[lo] + (pos - lo) * (copy[hi] - copy[lo]);
    }

    /** Median absolute deviation, κλιμακωμένο ώστε να συγκρίνεται με τυπική απόκλιση. */
    public static double mad(double[] raw) {
        double[] xs = present(raw);
        if (xs.length == 0) return Double.NaN;
        double med = median(xs);
        double[] deviations = new double[xs.length];
        for (int i = 0; i < xs.length; i++) deviations[i] = Math.abs(xs[i] - med);
        return median(deviations) * MAD_TO_SIGMA;
    }

    /**
     * Πόσο "παράξενη" είναι μια τιμή, σε ανθεκτικές τυπικές αποκλίσεις από τη διάμεσο.
     * Τιμή πάνω από ~3.5 θεωρείται ανωμαλία.
     *
     * @return 0 όταν η διασπορά είναι μηδενική (όλα ίδια), ώστε να μη βγαίνει άπειρο
     */
    public static double robustZ(double value, double median, double mad) {
        if (!Double.isFinite(value)) {
            return 0; // δεν μετρήθηκε: δεν υπάρχει τίποτα να χαρακτηριστεί παράξενο
        }
        if (!Double.isFinite(mad) || mad < 1e-12) {
            return Math.abs(value - median) < 1e-12 ? 0 : 0;
        }
        return (value - median) / mad;
    }

    /**
     * Εκθετικά σταθμισμένος κινούμενος μέσος: η "αναμενόμενη" πορεία μιας χρονοσειράς.
     * Η απόκλιση από αυτόν δείχνει αλλαγή συμπεριφοράς, ανεξάρτητα από το απόλυτο επίπεδο.
     *
     * @param alpha 0–1· μεγαλύτερο σημαίνει ότι ξεχνά πιο γρήγορα το παρελθόν
     */
    public static double[] ewma(double[] xs, double alpha) {
        double[] out = new double[xs.length];
        if (xs.length == 0) return out;
        out[0] = xs[0];
        for (int i = 1; i < xs.length; i++) {
            out[i] = alpha * xs[i] + (1 - alpha) * out[i - 1];
        }
        return out;
    }

    /**
     * Κλίση ευθείας ελαχίστων τετραγώνων ανά δείγμα — "ανεβαίνει ή κατεβαίνει;".
     * Κανονικοποιημένη στο μέσο επίπεδο, ώστε να είναι συγκρίσιμη μεταξύ πεδίων.
     */
    public static double trendPerSample(double[] raw) {
        double[] xs = present(raw);
        int n = xs.length;
        if (n < 3) return 0;
        double meanX = (n - 1) / 2.0;
        double meanY = mean(xs);
        double num = 0;
        double den = 0;
        for (int i = 0; i < n; i++) {
            num += (i - meanX) * (xs[i] - meanY);
            den += (i - meanX) * (i - meanX);
        }
        if (den < 1e-12) return 0;
        double slope = num / den;
        double scale = Math.abs(meanY) < 1e-9 ? 1 : Math.abs(meanY);
        return slope / scale;
    }
}
