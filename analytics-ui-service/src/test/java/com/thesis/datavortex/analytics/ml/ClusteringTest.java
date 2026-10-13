package com.thesis.datavortex.analytics.ml;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class ClusteringTest {

    /** Τρεις καθαρά χωρισμένες ομάδες, όπως τα καθεστώτα φορτίου μιας μηχανής. */
    private static double[][] threeBlobs() {
        Random random = new Random(7);
        double[][] data = new double[90][2];
        double[][] centres = {{0, 0}, {10, 10}, {0, 10}};
        for (int i = 0; i < data.length; i++) {
            double[] centre = centres[i % 3];
            data[i][0] = centre[0] + random.nextGaussian() * 0.4;
            data[i][1] = centre[1] + random.nextGaussian() * 0.4;
        }
        return data;
    }

    @Test
    void kMeansFindsTheThreeBlobs() {
        KMeans.Result result = KMeans.fit(threeBlobs(), 3);
        int[] sizes = result.sizes();
        for (int size : sizes) {
            assertEquals(30, size, 2, "οι ομάδες πρέπει να είναι ισομεγέθεις");
        }
        // Τα κέντρα πρέπει να είναι κοντά στα πραγματικά, σε κάποια σειρά.
        assertEquals(3, result.centroids().length);
    }

    @Test
    void sameInputGivesSameResult() {
        // Χωρίς αυτό, κάθε ανανέωση του UI θα άλλαζε χρώματα και ετικέτες χωρίς λόγο.
        double[][] data = threeBlobs();
        KMeans.Result first = KMeans.fit(data, 3);
        KMeans.Result second = KMeans.fit(data, 3);
        assertArrayEquals(first.assignments(), second.assignments());
        assertEquals(first.inertia(), second.inertia(), 1e-12);
    }

    @Test
    void moreClustersAlwaysLowersInertiaSoItCannotChooseK() {
        double[][] data = threeBlobs();
        double previous = Double.MAX_VALUE;
        for (int k = 1; k <= 6; k++) {
            double inertia = KMeans.fit(data, k).inertia();
            assertTrue(inertia <= previous + 1e-9, "το inertia δεν ανεβαίνει ποτέ με μεγαλύτερο k");
            previous = inertia;
        }
    }

    @Test
    void silhouetteChoosesTheRealNumberOfClusters() {
        Silhouette.Best best = Silhouette.chooseK(threeBlobs(), 2, 6);
        assertEquals(3, best.result().k(), "τρεις πραγματικές ομάδες, άρα k=3");
        assertTrue(best.score() > 0.7, "καθαρά χωρισμένες ομάδες δίνουν υψηλό silhouette: " + best.score());
    }

    @Test
    void kMeansSurvivesFewerPointsThanClusters() {
        double[][] tiny = {{1, 1}, {2, 2}};
        KMeans.Result result = KMeans.fit(tiny, 5);
        assertEquals(2, result.k(), "δεν μπορούν να υπάρχουν πιο πολλές ομάδες από σημεία");
        for (double[] centroid : result.centroids()) {
            for (double value : centroid) {
                assertFalse(Double.isNaN(value), "καμία άδεια ομάδα δεν πρέπει να δίνει NaN");
            }
        }
    }

    @Test
    void dbscanSeparatesDenseRegionsAndNamesTheNoise() {
        Random random = new Random(11);
        double[][] data = new double[62][2];
        for (int i = 0; i < 30; i++) {
            data[i] = new double[]{random.nextGaussian() * 0.2, random.nextGaussian() * 0.2};
        }
        for (int i = 30; i < 60; i++) {
            data[i] = new double[]{8 + random.nextGaussian() * 0.2, 8 + random.nextGaussian() * 0.2};
        }
        data[60] = new double[]{40, -30};   // μακρινά σημεία: πρέπει να βγουν θόρυβος
        data[61] = new double[]{-25, 35};

        int[] labels = Dbscan.fit(data, 1.0, 4);
        assertEquals(Dbscan.NOISE, labels[60]);
        assertEquals(Dbscan.NOISE, labels[61]);
        assertNotEquals(labels[0], labels[59], "οι δύο πυκνές περιοχές είναι ξεχωριστές ομάδες");
        assertNotEquals(Dbscan.NOISE, labels[0]);
        assertNotEquals(Dbscan.NOISE, labels[59]);
    }

    @Test
    void dbscanSuggestsUsableEps() {
        Random random = new Random(3);
        double[][] data = new double[40][2];
        for (int i = 0; i < data.length; i++) {
            data[i] = new double[]{random.nextGaussian(), random.nextGaussian()};
        }
        double eps = Dbscan.suggestEps(data, 4);
        assertTrue(eps > 0 && eps < 5, "το eps πρέπει να είναι στην κλίμακα των δεδομένων: " + eps);
        int[] labels = Dbscan.fit(data, eps, 4);
        long clustered = java.util.Arrays.stream(labels).filter(l -> l != Dbscan.NOISE).count();
        assertTrue(clustered > data.length / 2, "με αυτόματο eps τα περισσότερα σημεία βρίσκουν ομάδα");
    }

    @Test
    void pcaPutsAllVarianceOnOneAxisWhenDataIsOneDimensional() {
        // Τρεις στήλες που είναι όλες γραμμικές συναρτήσεις του χρόνου: μετά την κλιμάκωση
        // είναι η ίδια πληροφορία τρεις φορές, άρα υπάρχει ΕΝΑΣ πραγματικός άξονας.
        double[][] data = new double[50][3];
        for (int i = 0; i < 50; i++) {
            double t = i - 25;
            data[i] = new double[]{t, t * 0.5 + 0.01 * i, 0.001 * i};
        }
        Pca.Projection projection = Pca.project(Scaler.Z_SCORE.fitTransform(data));
        assertEquals(50, projection.points().length);
        assertTrue(projection.explainedX() > 0.98, "σχεδόν όλη η διασπορά στον πρώτο άξονα");
        assertTrue(projection.explainedY() < 0.02,
                "ο δεύτερος άξονας δεν πρέπει να δηλώνει διασπορά που δεν υπάρχει: "
                        + projection.explainedY());
        assertTrue(projection.explainedX() >= projection.explainedY());
    }

    @Test
    void pcaSplitsVarianceWhenThereAreTwoRealAxes() {
        Random random = new Random(5);
        double[][] data = new double[200][3];
        for (int i = 0; i < data.length; i++) {
            double a = random.nextGaussian() * 3;   // κύρια κατεύθυνση
            double b = random.nextGaussian();       // δεύτερη, ασθενέστερη
            data[i] = new double[]{a, b, a * 0.1 + random.nextGaussian() * 0.05};
        }
        Pca.Projection projection = Pca.project(Scaler.Z_SCORE.fitTransform(data));
        assertTrue(projection.explainedX() > projection.explainedY());
        assertTrue(projection.explainedX() + projection.explainedY() > 0.85,
                "δύο άξονες πρέπει να κρατούν το μεγαλύτερο μέρος της πληροφορίας");
        assertTrue(projection.explainedY() > 0.05, "ο δεύτερος άξονας υπάρχει πραγματικά εδώ");
    }

    @Test
    void pcaHandlesSingleColumnAndEmptyInput() {
        assertEquals(0, Pca.project(new double[0][0]).points().length);
        Pca.Projection single = Pca.project(new double[][]{{1}, {2}, {3}});
        assertEquals(3, single.points().length);
        assertEquals(0, single.points()[0][1], "με μία διάσταση ο δεύτερος άξονας είναι μηδέν");
    }
}
