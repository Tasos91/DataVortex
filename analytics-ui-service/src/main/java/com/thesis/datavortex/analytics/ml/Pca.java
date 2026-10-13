package com.thesis.datavortex.analytics.ml;

/**
 * Προβολή σε δύο διαστάσεις, για να μπορεί να ζωγραφιστεί το clustering.
 * <p>
 * Τα δεδομένα έχουν 5–6 χαρακτηριστικά· η οθόνη έχει δύο. Η PCA βρίσκει τους δύο άξονες
 * στους οποίους τα δεδομένα απλώνονται περισσότερο, άρα το διάγραμμα δείχνει τη μέγιστη
 * δυνατή πληροφορία. Οι ιδιοδιανύσματα βρίσκονται με power iteration και αφαίρεση του
 * πρώτου άξονα (deflation) — αρκεί για δύο άξονες και αποφεύγει εξωτερική βιβλιοθήκη.
 */
public final class Pca {

    private static final int ITERATIONS = 200;

    private Pca() {}

    /**
     * @param scaled κλιμακωμένα δεδομένα (η PCA είναι ευαίσθητη στις μονάδες)
     * @return ζεύγη (x, y) ανά δείγμα, και πόση διασπορά εξηγεί ο κάθε άξονας
     */
    public static Projection project(double[][] scaled) {
        if (scaled.length == 0 || scaled[0].length == 0) {
            return new Projection(new double[0][2], 0, 0);
        }
        int d = scaled[0].length;
        if (d == 1) {
            double[][] points = new double[scaled.length][2];
            for (int i = 0; i < scaled.length; i++) {
                points[i][0] = scaled[i][0];
            }
            return new Projection(points, 1, 0);
        }

        double[][] covariance = covariance(scaled);
        double totalVariance = 0;
        for (int i = 0; i < d; i++) {
            totalVariance += covariance[i][i];
        }

        double[] first = dominantEigenvector(covariance);
        double firstValue = rayleigh(covariance, first);
        double[][] deflated = deflate(covariance, first, firstValue);
        double[] second = dominantEigenvector(deflated);
        // Η ιδιοτιμή του δεύτερου άξονα μετριέται στον ΑΠΟΜΕΙΩΜΕΝΟ πίνακα. Με τον αρχικό, σε
        // δεδομένα που απλώνονται σε μία μόνο κατεύθυνση ο δεύτερος άξονας θα "δανειζόταν" τη
        // διασπορά του πρώτου και θα δηλωνόταν διασπορά που δεν υπάρχει.
        double secondValue = Math.min(rayleigh(deflated, second), firstValue);

        double[][] points = new double[scaled.length][2];
        for (int i = 0; i < scaled.length; i++) {
            points[i][0] = dot(scaled[i], first);
            points[i][1] = dot(scaled[i], second);
        }
        double denominator = totalVariance > 1e-12 ? totalVariance : 1;
        return new Projection(points, firstValue / denominator, secondValue / denominator);
    }

    public record Projection(double[][] points, double explainedX, double explainedY) {}

    private static double[][] covariance(double[][] data) {
        int n = data.length;
        int d = data[0].length;
        double[] means = new double[d];
        for (double[] row : data) {
            for (int j = 0; j < d; j++) means[j] += row[j];
        }
        for (int j = 0; j < d; j++) means[j] /= n;

        double[][] cov = new double[d][d];
        for (double[] row : data) {
            for (int a = 0; a < d; a++) {
                for (int b = a; b < d; b++) {
                    cov[a][b] += (row[a] - means[a]) * (row[b] - means[b]);
                }
            }
        }
        int divisor = Math.max(1, n - 1);
        for (int a = 0; a < d; a++) {
            for (int b = a; b < d; b++) {
                cov[a][b] /= divisor;
                cov[b][a] = cov[a][b];
            }
        }
        return cov;
    }

    private static double[] dominantEigenvector(double[][] matrix) {
        int d = matrix.length;
        double[] v = new double[d];
        // Ντετερμινιστική αρχική τιμή: το ίδιο input δίνει πάντα το ίδιο διάγραμμα.
        for (int i = 0; i < d; i++) {
            v[i] = 1.0 / Math.sqrt(d);
        }
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            double[] next = new double[d];
            for (int a = 0; a < d; a++) {
                for (int b = 0; b < d; b++) {
                    next[a] += matrix[a][b] * v[b];
                }
            }
            double norm = Math.sqrt(dot(next, next));
            if (norm < 1e-12) {
                return v;
            }
            for (int a = 0; a < d; a++) {
                next[a] /= norm;
            }
            v = next;
        }
        return v;
    }

    private static double[][] deflate(double[][] matrix, double[] vector, double value) {
        int d = matrix.length;
        double[][] out = new double[d][d];
        for (int a = 0; a < d; a++) {
            for (int b = 0; b < d; b++) {
                out[a][b] = matrix[a][b] - value * vector[a] * vector[b];
            }
        }
        return out;
    }

    private static double rayleigh(double[][] matrix, double[] v) {
        int d = matrix.length;
        double[] mv = new double[d];
        for (int a = 0; a < d; a++) {
            for (int b = 0; b < d; b++) {
                mv[a] += matrix[a][b] * v[b];
            }
        }
        return Math.max(0, dot(v, mv));
    }

    private static double dot(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length && i < b.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }
}
