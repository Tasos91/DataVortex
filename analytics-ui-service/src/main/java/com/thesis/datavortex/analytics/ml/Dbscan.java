package com.thesis.datavortex.analytics.ml;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * DBSCAN: ομαδοποίηση με βάση την πυκνότητα.
 * <p>
 * Χρησιμοποιείται για δεδομένα που δεν ξέρουμε τι σχήμα έχουν (το προφίλ {@code generic}).
 * Σε αντίθεση με το k-means:
 * <ul>
 *   <li>δεν χρειάζεται να πεις πόσες ομάδες υπάρχουν,</li>
 *   <li>βρίσκει ομάδες με οποιοδήποτε σχήμα, όχι μόνο σφαιρικές,</li>
 *   <li>και κυρίως: χαρακτηρίζει σημεία ως <b>θόρυβο</b>, δηλαδή τα βρίσκει τα outliers
 *       αντί να τα στριμώξει με το ζόρι σε κάποια ομάδα.</li>
 * </ul>
 */
public final class Dbscan {

    public static final int NOISE = -1;

    private Dbscan() {}

    /**
     * @param eps      ακτίνα γειτονιάς στον κλιμακωμένο χώρο
     * @param minPts   πόσα σημεία χρειάζονται μέσα στην ακτίνα για να θεωρηθεί πυκνή περιοχή
     * @return ετικέτα ομάδας ανά σημείο, με {@link #NOISE} για τα outliers
     */
    public static int[] fit(double[][] data, double eps, int minPts) {
        int n = data.length;
        int[] labels = new int[n];
        java.util.Arrays.fill(labels, -2); // -2 = δεν εξετάστηκε ακόμα
        int cluster = 0;

        for (int i = 0; i < n; i++) {
            if (labels[i] != -2) {
                continue;
            }
            List<Integer> neighbours = neighbours(data, i, eps);
            if (neighbours.size() < minPts) {
                labels[i] = NOISE; // μπορεί να γίνει μέλος αργότερα, ως σύνορο
                continue;
            }
            labels[i] = cluster;
            Deque<Integer> queue = new ArrayDeque<>(neighbours);
            while (!queue.isEmpty()) {
                int j = queue.poll();
                if (labels[j] == NOISE) {
                    labels[j] = cluster; // σημείο συνόρου
                }
                if (labels[j] != -2) {
                    continue;
                }
                labels[j] = cluster;
                List<Integer> more = neighbours(data, j, eps);
                if (more.size() >= minPts) {
                    queue.addAll(more);
                }
            }
            cluster++;
        }
        return labels;
    }

    /**
     * Το αποτέλεσμα της αυτόματης επιλογής.
     *
     * @param quality καθαρότητα διαχωρισμού μείον ποινές για θόρυβο και κατακερματισμό
     */
    public record Fit(int[] labels, double eps, int clusters, int noise, double quality) {}

    /** Υποψήφια ποσοστημόρια της k-distance· από σφιχτό προς χαλαρό. */
    private static final double[] CANDIDATES = {0.50, 0.60, 0.70, 0.80, 0.90, 0.95};

    /**
     * Διαλέγει το {@code eps} με κριτήριο, αντί να το μαντέψει.
     * <p>
     * Το DBSCAN είναι εξαιρετικά ευαίσθητο στο eps: λίγο μικρό και η ίδια ομάδα σπάει σε
     * οκτώ κομμάτια, λίγο μεγάλο και δύο διαφορετικές συμπεριφορές γίνονται μία. Ένα σταθερό
     * ποσοστημόριο της k-distance δεν αρκεί, γι' αυτό δοκιμάζουμε αρκετά και κρατάμε αυτό που
     * δίνει καθαρό διαχωρισμό, λίγο θόρυβο και όχι κατακερματισμό.
     */
    public static Fit fitAuto(double[][] data, int minPts) {
        if (data.length <= minPts) {
            int[] labels = new int[data.length];
            return new Fit(labels, 1.0, data.length == 0 ? 0 : 1, 0, 0);
        }
        // Μόνο οι θετικές αποστάσεις: όταν πολλά σημεία είναι πανομοιότυπα (συχνό σε
        // στρογγυλεμένες μετρήσεις), οι μηδενικές k-distances θα έσπρωχναν όλα τα
        // ποσοστημόρια στο μηδέν και το eps θα ήταν άχρηστο.
        double[] kDistances = java.util.Arrays.stream(kDistances(data, minPts))
                .filter(d -> d > 1e-12).toArray();
        if (kDistances.length < 2) {
            kDistances = kDistances(data, minPts);
        }
        Fit best = null;
        for (double quantile : CANDIDATES) {
            double eps = Stats.quantile(kDistances, quantile);
            if (!(eps > 1e-9)) {
                continue;
            }
            int[] labels = fit(data, eps, minPts);
            int clusters = 0;
            int noise = 0;
            for (int label : labels) {
                clusters = Math.max(clusters, label + 1);
                if (label == NOISE) {
                    noise++;
                }
            }
            if (clusters == 0) {
                continue;
            }
            double noiseShare = (double) noise / data.length;
            // Ποινή κατακερματισμού: δέκα μικρές ομάδες δεν είναι καλύτερη εξήγηση από δύο.
            double quality = separation(data, labels, clusters) - 2 * noiseShare - 0.05 * (clusters - 1);
            if (best == null || quality > best.quality()) {
                best = new Fit(labels, eps, clusters, noise, quality);
            }
        }
        return best != null ? best : new Fit(new int[data.length], 1.0, 1, 0, 0);
    }

    /**
     * Πόσο καλά χωρισμένες είναι οι ομάδες: μέσος όρος του {@code (b - a) / max(a, b)} πάνω στα
     * σημεία που ανήκουν σε ομάδα, όπου a = απόσταση από το δικό τους κέντρο και b = από το
     * πλησιέστερο ξένο. Είναι ο δείκτης silhouette υπολογισμένος στα κέντρα.
     */
    private static double separation(double[][] data, int[] labels, int clusters) {
        if (clusters < 2) {
            return 0;
        }
        int dimensions = data[0].length;
        double[][] centroids = new double[clusters][dimensions];
        int[] counts = new int[clusters];
        for (int i = 0; i < data.length; i++) {
            if (labels[i] == NOISE) {
                continue;
            }
            counts[labels[i]]++;
            for (int d = 0; d < dimensions; d++) {
                centroids[labels[i]][d] += data[i][d];
            }
        }
        for (int c = 0; c < clusters; c++) {
            for (int d = 0; d < dimensions && counts[c] > 0; d++) {
                centroids[c][d] /= counts[c];
            }
        }
        double total = 0;
        int considered = 0;
        for (int i = 0; i < data.length; i++) {
            if (labels[i] == NOISE) {
                continue;
            }
            double a = KMeans.distance(data[i], centroids[labels[i]]);
            double b = Double.MAX_VALUE;
            for (int c = 0; c < clusters; c++) {
                if (c != labels[i] && counts[c] > 0) {
                    b = Math.min(b, KMeans.distance(data[i], centroids[c]));
                }
            }
            double denominator = Math.max(a, b);
            total += denominator < 1e-12 ? 0 : (b - a) / denominator;
            considered++;
        }
        return considered == 0 ? 0 : total / considered;
    }

    /** Απόσταση κάθε σημείου από τον {@code minPts}-στό γείτονά του. */
    private static double[] kDistances(double[][] data, int minPts) {
        int n = data.length;
        double[] kDistances = new double[n];
        for (int i = 0; i < n; i++) {
            double[] distances = new double[n - 1];
            int idx = 0;
            for (int j = 0; j < n; j++) {
                if (i != j) {
                    distances[idx++] = KMeans.distance(data[i], data[j]);
                }
            }
            java.util.Arrays.sort(distances);
            kDistances[i] = distances[Math.min(minPts - 1, distances.length - 1)];
        }
        return kDistances;
    }



    private static List<Integer> neighbours(double[][] data, int index, double eps) {
        List<Integer> found = new ArrayList<>();
        double epsSquared = eps * eps;
        for (int j = 0; j < data.length; j++) {
            if (j != index && KMeans.squaredDistance(data[index], data[j]) <= epsSquared) {
                found.add(j);
            }
        }
        return found;
    }
}
