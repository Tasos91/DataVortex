package com.thesis.datavortex.analytics.profile;

import com.thesis.datavortex.analytics.ml.Dbscan;
import com.thesis.datavortex.analytics.ml.Pca;
import com.thesis.datavortex.analytics.ml.Scaler;
import com.thesis.datavortex.analytics.ml.Stats;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Το προφίλ για δεδομένα που δεν αναγνωρίζουμε.
 * <p>
 * Εδώ δεν ξέρουμε ούτε τι σημαίνουν τα πεδία ούτε πόσες ομάδες υπάρχουν, άρα δεν μπορούμε να
 * επιβάλουμε μοντέλο. Χρησιμοποιεί <b>DBSCAN</b> και όχι k-means, επειδή:
 * <ul>
 *   <li>το k-means απαιτεί να πεις πόσες ομάδες υπάρχουν — εδώ θα ήταν μαντεψιά,</li>
 *   <li>το k-means στριμώχνει ΚΑΘΕ σημείο σε μια ομάδα, άρα κρύβει τα outliers μέσα τους,
 *       ενώ το DBSCAN τα ονομάζει <i>θόρυβο</i>, που εδώ είναι ακριβώς το ζητούμενο,</li>
 *   <li>και βρίσκει ομάδες με οποιοδήποτε σχήμα, όχι μόνο σφαιρικές.</li>
 * </ul>
 * Οι κατηγορίες είναι συνεπώς «ομάδα Ν» και «απομακρυσμένο»: ό,τι μπορεί να ειπωθεί τίμια
 * χωρίς γνώση του τι μετράμε.
 */
@Component
public class GenericProfile implements DataProfile {

    private static final int MIN_POINTS = 4;
    private static final double ANOMALY_THRESHOLD = 3.5;

    @Override
    public String id() {
        return "generic";
    }

    @Override
    public String title() {
        return "Γενικά αριθμητικά δεδομένα";
    }

    @Override
    public String approach() {
        return "DBSCAN χωρίς προκαθορισμένο αριθμό ομάδων, με ρητό εντοπισμό απομακρυσμένων σημείων";
    }

    @Override
    public int score(String sourceId, Map<String, FieldRoles> roles) {
        return 1; // ταιριάζει πάντα, αλλά χάνει από κάθε εξειδικευμένο προφίλ
    }

    @Override
    public Analysis analyse(Input input) {
        if (input.size() < 4 || input.fields().isEmpty()) {
            return Profiles.empty(this, input, "Πολύ λίγα σημεία για ανάλυση σε αυτό το διάστημα.");
        }
        List<String> features = input.fields();
        double[][] raw = input.matrix(features);
        double[][] scaled = Scaler.ROBUST.fitTransform(raw);

        Dbscan.Fit fit = Dbscan.fitAuto(scaled, MIN_POINTS);
        int[] labels = fit.labels();
        double eps = fit.eps();
        int clusterCount = fit.clusters();
        Pca.Projection projection = Pca.project(scaled);

        // Ομάδες σε πραγματικές μονάδες, και ο θόρυβος ξεχωριστά.
        List<Analysis.Cluster> clusters = new ArrayList<>();
        for (int c = 0; c < clusterCount; c++) {
            clusters.add(describe(c, "ομάδα " + (c + 1), labels, c, raw, features));
        }
        int noise = (int) java.util.Arrays.stream(labels).filter(l -> l == Dbscan.NOISE).count();
        if (noise > 0) {
            clusters.add(describe(clusterCount, "απομακρυσμένα", labels, Dbscan.NOISE, raw, features));
        }

        // Διάμεσος και MAD μία φορά ανά πεδίο, έξω από τον βρόχο των σημείων: μέσα θα ήταν
        // δύο ταξινομήσεις ανά σημείο ανά πεδίο, δηλαδή O(n² log n).
        double[][] columns = new double[features.size()][];
        double[] medians = new double[features.size()];
        double[] mads = new double[features.size()];
        for (int f = 0; f < features.size(); f++) {
            columns[f] = input.column(features.get(f));
            medians[f] = Stats.median(columns[f]);
            mads[f] = Stats.mad(columns[f]);
        }

        double[] anomalyScores = new double[input.size()];
        for (int i = 0; i < input.size(); i++) {
            double worst = 0;
            for (int f = 0; f < columns.length; f++) {
                worst = Math.max(worst, Math.abs(Stats.robustZ(columns[f][i], medians[f], mads[f])));
            }
            // Ο θόρυβος του DBSCAN είναι από μόνος του ένδειξη, ακόμα κι όταν κανένα
            // μεμονωμένο πεδίο δεν φαίνεται ακραίο: μπορεί να είναι ο ΣΥΝΔΥΑΣΜΟΣ που είναι σπάνιος.
            anomalyScores[i] = labels[i] == Dbscan.NOISE ? Math.max(worst, ANOMALY_THRESHOLD) : worst;
        }

        List<String> classes = new ArrayList<>(input.size());
        List<String> clusterLabels = clusters.stream().map(Analysis.Cluster::label).toList();
        for (int label : labels) {
            classes.add(label == Dbscan.NOISE ? "απομακρυσμένο" : "ομάδα " + (label + 1));
        }

        List<Analysis.Assignment> assignments = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            int cluster = labels[i] == Dbscan.NOISE ? clusterCount : labels[i];
            assignments.add(new Analysis.Assignment(input.times().get(i), cluster,
                    cluster < clusterLabels.size() ? clusterLabels.get(cluster) : "απομακρυσμένα",
                    classes.get(i),
                    Profiles.round(projection.points()[i][0]), Profiles.round(projection.points()[i][1]),
                    Profiles.round(anomalyScores[i])));
        }

        // Χωρίς γνώση των πεδίων, κοιτάμε και τις αιχμές: εκεί κρύβονται τα outliers.
        List<Analysis.Anomaly> anomalies = new ArrayList<>(
                Profiles.robustAnomalies(input, features, ANOMALY_THRESHOLD, 2, false));
        anomalies.addAll(Profiles.robustAnomalies(input, features, ANOMALY_THRESHOLD, 2, true));
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == Dbscan.NOISE) {
                anomalies.add(new Analysis.Anomaly(input.times().get(i), "(συνδυασμός πεδίων)",
                        0, 0, Profiles.round(anomalyScores[i]),
                        "σπάνιος συνδυασμός τιμών: το DBSCAN δεν το έβαλε σε καμία πυκνή περιοχή"));
            }
        }
        anomalies.sort(Comparator.comparingDouble(Analysis.Anomaly::score).reversed());

        List<Analysis.ClassCount> classCounts = Profiles.countClasses(classes);
        double noiseShare = 100.0 * noise / Math.max(1, input.size());
        return new Analysis(input.source(), id(), title(), input.bucket(), input.from(), input.to(),
                input.size(), features,
                "DBSCAN (eps %.2f, minPts %d) → %d ομάδες, %d απομακρυσμένα"
                        .formatted(eps, MIN_POINTS, clusterCount, noise),
                clusters, assignments, anomalies.subList(0, Math.min(25, anomalies.size())), classCounts,
                verdict(noiseShare, clusterCount, anomalies),
                Profiles.round(projection.explainedX() * 100), Profiles.round(projection.explainedY() * 100));
    }

    private Analysis.Cluster describe(int id, String label, int[] labels, int match,
                                      double[][] raw, List<String> features) {
        Map<String, Double> centroid = new LinkedHashMap<>();
        int size = 0;
        for (int i = 0; i < labels.length; i++) {
            if (labels[i] == match) {
                size++;
            }
        }
        for (int f = 0; f < features.size(); f++) {
            double sum = 0;
            for (int i = 0; i < labels.length; i++) {
                if (labels[i] == match) {
                    sum += raw[i][f];
                }
            }
            centroid.put(features.get(f), size == 0 ? 0.0 : Profiles.round(sum / size));
        }
        String headline = centroid.entrySet().stream().limit(2)
                .map(e -> e.getKey() + " " + e.getValue())
                .reduce((a, b) -> a + ", " + b).orElse("—");
        return new Analysis.Cluster(id, label, size,
                Profiles.round(100.0 * size / Math.max(1, labels.length)), centroid, headline);
    }

    private Analysis.Verdict verdict(double noiseShare, int clusterCount, List<Analysis.Anomaly> anomalies) {
        int score = (int) Math.max(0, Math.min(100, Math.round(100 - noiseShare * 2)));
        List<String> findings = new ArrayList<>();
        findings.add(switch (clusterCount) {
            case 0 -> "Δεν βρέθηκαν πυκνές περιοχές: τα δεδομένα είναι διάσπαρτα.";
            case 1 -> "Τα δεδομένα δείχνουν μία ενιαία συμπεριφορά.";
            default -> "Βρέθηκαν %d διακριτές συμπεριφορές στα δεδομένα.".formatted(clusterCount);
        });
        if (noiseShare > 1) {
            findings.add("Το %.0f%% των κουβάδων δεν ανήκει σε καμία πυκνή περιοχή.".formatted(noiseShare));
        }
        if (!anomalies.isEmpty()) {
            findings.add("%d σημεία ξεχωρίζουν· το εντονότερο: %s."
                    .formatted(anomalies.size(), anomalies.get(0).reason()));
        }
        findings.add("Τα πεδία δεν αναγνωρίστηκαν, άρα δεν αποδίδεται νόημα στις τιμές — "
                + "δώσε στα πεδία πιο περιγραφικά ονόματα για πιο στοχευμένη ανάλυση.");
        String status = score >= 85 ? "OK" : score >= 60 ? "WATCH" : "ALERT";
        return new Analysis.Verdict(score, status,
                status.equals("OK") ? "Ομοιογενή δεδομένα" : "Ανομοιογενή δεδομένα", findings);
    }
}
