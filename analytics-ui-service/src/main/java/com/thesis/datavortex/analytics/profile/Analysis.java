package com.thesis.datavortex.analytics.profile;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Ό,τι βγάζει μια ανάλυση, σε μορφή που μπορεί να ζωγραφιστεί κατευθείαν.
 * Ίδιο σχήμα για όλα τα προφίλ, ώστε το UI να μη ξέρει τι τύπο δεδομένων κοιτάζει.
 */
public record Analysis(
        String source,
        String profile,
        String profileTitle,
        String bucket,
        Instant from,
        Instant to,
        int points,
        List<String> features,
        String algorithm,
        List<Cluster> clusters,
        List<Assignment> assignments,
        List<Anomaly> anomalies,
        List<ClassCount> classes,
        Verdict verdict,
        double explainedX,
        double explainedY) {

    /**
     * Μια ομάδα (καθεστώς λειτουργίας).
     *
     * @param centroid το κέντρο σε ΠΡΑΓΜΑΤΙΚΕΣ μονάδες, όχι κλιμακωμένες — αλλιώς δεν λέει
     *                 τίποτα σε άνθρωπο
     * @param headline σε τι ξεχωρίζει αυτή η ομάδα, σε μια φράση
     */
    public record Cluster(int id, String label, int size, double share,
                          Map<String, Double> centroid, String headline) {}

    /**
     * Ένας χρονικός κουβάς τοποθετημένος σε ομάδα και κατηγορία.
     *
     * @param x,y προβολή PCA, για το διάγραμμα διασποράς
     */
    public record Assignment(Instant t, int cluster, String label, String className,
                             double x, double y, double anomalyScore) {}

    /**
     * @param expected τι θα περιμέναμε εκεί (διάμεσος ή εποχικό baseline)
     * @param score    πόσες ανθεκτικές τυπικές αποκλίσεις μακριά
     * @param reason   γιατί σημάνθηκε, σε απλά λόγια
     */
    public record Anomaly(Instant t, String field, double value, double expected,
                          double score, String reason) {}

    public record ClassCount(String className, int count, double share) {}

    /**
     * Η συνολική κρίση.
     *
     * @param score    0–100 (100 = όλα ήρεμα)
     * @param status   OK / WATCH / ALERT
     * @param findings συγκεκριμένες διαπιστώσεις, η σειρά έχει σημασία
     */
    public record Verdict(int score, String status, String headline, List<String> findings) {}
}
