package com.thesis.datavortex.analytics.ml;

import java.util.Arrays;
import java.util.Random;

/**
 * k-means με αρχικοποίηση k-means++ και πολλαπλές εκκινήσεις.
 * <p>
 * Το απλό k-means με τυχαία αρχικά κέντρα δίνει διαφορετικό αποτέλεσμα σε κάθε εκτέλεση και
 * συχνά κολλάει σε κακό τοπικό ελάχιστο. Το k-means++ διαλέγει αρχικά κέντρα μακριά μεταξύ
 * τους, και κρατάμε την καλύτερη από πολλές εκκινήσεις. Ο σπόρος είναι σταθερός, ώστε το ίδιο
 * παράθυρο δεδομένων να δίνει πάντα το ίδιο αποτέλεσμα — αλλιώς το UI θα "χοροπηδούσε".
 */
public final class KMeans {

    private static final int MAX_ITERATIONS = 100;
    private static final int RESTARTS = 8;

    private KMeans() {}

    /**
     * @param assignments σε ποια ομάδα ανήκει κάθε δείγμα
     * @param centroids   τα κέντρα των ομάδων, στον κλιμακωμένο χώρο
     * @param inertia     άθροισμα τετραγωνικών αποστάσεων από το κέντρο (μικρότερο = σφιχτότερο)
     */
    public record Result(int[] assignments, double[][] centroids, double inertia, int k) {

        public int[] sizes() {
            int[] sizes = new int[k];
            for (int a : assignments) sizes[a]++;
            return sizes;
        }
    }

    public static Result fit(double[][] data, int k) {
        if (data.length == 0 || k <= 0) {
            return new Result(new int[0], new double[0][0], 0, 0);
        }
        int effectiveK = Math.min(k, data.length);
        Result best = null;
        for (int restart = 0; restart < RESTARTS; restart++) {
            Result candidate = runOnce(data, effectiveK, new Random(42L + restart * 7919L));
            if (best == null || candidate.inertia() < best.inertia()) {
                best = candidate;
            }
        }
        return best;
    }

    private static Result runOnce(double[][] data, int k, Random random) {
        double[][] centroids = kmeansPlusPlusInit(data, k, random);
        int[] assignments = new int[data.length];
        Arrays.fill(assignments, -1);

        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            boolean moved = false;
            for (int i = 0; i < data.length; i++) {
                int nearest = nearest(data[i], centroids);
                if (assignments[i] != nearest) {
                    assignments[i] = nearest;
                    moved = true;
                }
            }
            if (!moved) {
                break; // σύγκλιση
            }
            centroids = recomputeCentroids(data, assignments, centroids, k);
        }

        double inertia = 0;
        for (int i = 0; i < data.length; i++) {
            inertia += squaredDistance(data[i], centroids[assignments[i]]);
        }
        return new Result(assignments, centroids, inertia, k);
    }

    /** Πρώτο κέντρο τυχαία, κάθε επόμενο με πιθανότητα ανάλογη της απόστασης από τα υπάρχοντα. */
    private static double[][] kmeansPlusPlusInit(double[][] data, int k, Random random) {
        double[][] centroids = new double[k][];
        centroids[0] = data[random.nextInt(data.length)].clone();

        double[] closest = new double[data.length];
        Arrays.fill(closest, Double.MAX_VALUE);

        for (int c = 1; c < k; c++) {
            double total = 0;
            for (int i = 0; i < data.length; i++) {
                closest[i] = Math.min(closest[i], squaredDistance(data[i], centroids[c - 1]));
                total += closest[i];
            }
            if (total <= 0) {
                centroids[c] = data[random.nextInt(data.length)].clone();
                continue;
            }
            double target = random.nextDouble() * total;
            double running = 0;
            int chosen = data.length - 1;
            for (int i = 0; i < data.length; i++) {
                running += closest[i];
                if (running >= target) {
                    chosen = i;
                    break;
                }
            }
            centroids[c] = data[chosen].clone();
        }
        return centroids;
    }

    private static double[][] recomputeCentroids(double[][] data, int[] assignments,
                                                 double[][] previous, int k) {
        int dimensions = data[0].length;
        double[][] sums = new double[k][dimensions];
        int[] counts = new int[k];
        for (int i = 0; i < data.length; i++) {
            counts[assignments[i]]++;
            for (int d = 0; d < dimensions; d++) {
                sums[assignments[i]][d] += data[i][d];
            }
        }
        double[][] centroids = new double[k][dimensions];
        for (int c = 0; c < k; c++) {
            if (counts[c] == 0) {
                // Άδεια ομάδα: κρατάμε το προηγούμενο κέντρο αντί να παράγουμε NaN.
                centroids[c] = previous[c].clone();
                continue;
            }
            for (int d = 0; d < dimensions; d++) {
                centroids[c][d] = sums[c][d] / counts[c];
            }
        }
        return centroids;
    }

    public static int nearest(double[] point, double[][] centroids) {
        int best = 0;
        double bestDistance = Double.MAX_VALUE;
        for (int c = 0; c < centroids.length; c++) {
            double distance = squaredDistance(point, centroids[c]);
            if (distance < bestDistance) {
                bestDistance = distance;
                best = c;
            }
        }
        return best;
    }

    public static double squaredDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length && i < b.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double distance(double[] a, double[] b) {
        return Math.sqrt(squaredDistance(a, b));
    }
}
