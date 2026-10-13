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

    /**
     * Δοκιμάζει k από {@code minK} έως {@code maxK} και κρατά το καλύτερο silhouette.
     *
     * @return το καλύτερο αποτέλεσμα, μαζί με τον δείκτη του
     */
    public static Best chooseK(double[][] data, int minK, int maxK) {
        int upper = Math.min(maxK, Math.max(1, data.length - 1));
        KMeans.Result best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (int k = Math.max(2, minK); k <= upper; k++) {
            KMeans.Result candidate = KMeans.fit(data, k);
            double score = score(data, candidate);
            if (score > bestScore) {
                bestScore = score;
                best = candidate;
            }
        }
        if (best == null) {
            // Λιγότερα δείγματα από ομάδες: μία ομάδα είναι η μόνη έντιμη απάντηση.
            return new Best(KMeans.fit(data, 1), -1);
        }
        return new Best(best, bestScore);
    }

    public record Best(KMeans.Result result, double score) {}
}
