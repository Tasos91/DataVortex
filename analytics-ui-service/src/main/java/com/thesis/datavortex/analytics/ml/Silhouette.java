package com.thesis.datavortex.analytics.ml;

/**
 * Επιλογή του k χωρίς να το μαντέψουμε.
 * <p>
 * Το inertia πέφτει πάντα όσο μεγαλώνει το k, άρα δεν μπορεί να διαλέξει k. Ο δείκτης
 * silhouette μετρά πόσο πιο κοντά είναι κάθε σημείο στη δική του ομάδα απ' ό,τι στην
 * πλησιέστερη ξένη: 1 σημαίνει καθαρά διαχωρισμένες ομάδες, 0 επικαλυπτόμενες.
 * <p>
 * Υπολογίζεται πάνω στα κέντρα (αντί σε όλα τα ζεύγη σημείων) ώστε να μένει O(n·k)
 * και να μη γίνεται O(n²) όταν μεγαλώσουν τα δεδομένα.
 */
public final class Silhouette {

    private Silhouette() {}

    public static double score(double[][] data, KMeans.Result result) {
        if (result.k() < 2 || data.length <= result.k()) {
            return -1;
        }
        double total = 0;
        for (int i = 0; i < data.length; i++) {
            int own = result.assignments()[i];
            double a = KMeans.distance(data[i], result.centroids()[own]);
            double b = Double.MAX_VALUE;
            for (int c = 0; c < result.k(); c++) {
                if (c != own) {
                    b = Math.min(b, KMeans.distance(data[i], result.centroids()[c]));
                }
            }
            double denominator = Math.max(a, b);
            total += denominator < 1e-12 ? 0 : (b - a) / denominator;
        }
        return total / data.length;
    }

    /** Πάνω από τόσα σημεία, η αναζήτηση του k γίνεται σε δείγμα. */
    private static final int SEARCH_SAMPLE = 250;
    /** Στην αναζήτηση αρκούν δύο εκκινήσεις· η τελική προσαρμογή παίρνει τις κανονικές. */
    private static final int SEARCH_RESTARTS = 2;

    /**
     * Δοκιμάζει k από {@code minK} έως {@code maxK} και κρατά το καλύτερο silhouette.
     * <p>
     * Δύο επιταχύνσεις που δεν αλλάζουν το αποτέλεσμα σε τίποτα ουσιαστικό:
     * <ul>
     *   <li>η <b>αναζήτηση</b> γίνεται σε δείγμα (κάθε k-οστό σημείο) όταν τα δεδομένα είναι
     *       πολλά — το ποιο k ταιριάζει δεν αλλάζει αν κοιτάξεις 400 αντί για 4.000 σημεία·</li>
     *   <li>σταματά όταν ο δείκτης πέσει δύο φορές συνεχόμενα, αφού είναι σχεδόν πάντα
     *       μονοκόρυφος ως συνάρτηση του k.</li>
     * </ul>
     * Η τελική προσαρμογή γίνεται πάντα στα <b>πλήρη</b> δεδομένα.
     */
    public static Best chooseK(double[][] data, int minK, int maxK) {
        // Τουλάχιστον ~8 σημεία ανά ομάδα: αλλιώς "βρίσκουμε" ομάδες από ένα-δύο σημεία, που
        // δεν είναι μοτίβο αλλά θόρυβος με όνομα.
        int upper = Math.min(maxK, Math.max(2, data.length / 8));
        upper = Math.min(upper, Math.max(1, data.length - 1));
        double[][] sample = subsample(data);

        int bestK = -1;
        double bestScore = Double.NEGATIVE_INFINITY;
        int drops = 0;
        for (int k = Math.max(2, minK); k <= upper; k++) {
            double score = score(sample, KMeans.fit(sample, k, SEARCH_RESTARTS));
            if (score > bestScore) {
                bestScore = score;
                bestK = k;
                drops = 0;
            } else if (++drops >= 2) {
                break;
            }
        }
        if (bestK < 0) {
            // Λιγότερα δείγματα από ομάδες: μία ομάδα είναι η μόνη έντιμη απάντηση.
            return new Best(KMeans.fit(data, 1), -1);
        }
        KMeans.Result result = KMeans.fit(data, bestK);
        return new Best(result, sample.length == data.length ? bestScore : score(data, result));
    }

    /** Κάθε n-οστό σημείο· ντετερμινιστικό, ώστε το ίδιο input να δίνει το ίδιο k. */
    private static double[][] subsample(double[][] data) {
        if (data.length <= SEARCH_SAMPLE) {
            return data;
        }
        int stride = (int) Math.ceil((double) data.length / SEARCH_SAMPLE);
        double[][] sample = new double[(data.length + stride - 1) / stride][];
        for (int i = 0, j = 0; i < data.length && j < sample.length; i += stride, j++) {
            sample[j] = data[i];
        }
        return sample;
    }

    public record Best(KMeans.Result result, double score) {}
}
