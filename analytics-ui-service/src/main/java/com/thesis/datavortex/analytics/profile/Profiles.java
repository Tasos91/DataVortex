package com.thesis.datavortex.analytics.profile;

import com.thesis.datavortex.analytics.ml.KMeans;
import com.thesis.datavortex.analytics.ml.Pca;
import com.thesis.datavortex.analytics.ml.Scaler;
import com.thesis.datavortex.analytics.ml.Silhouette;
import com.thesis.datavortex.analytics.ml.Stats;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Κοινά βήματα που μοιράζονται τα προφίλ, ώστε η διαφορά τους να είναι μόνο η γνώση τους. */
public final class Profiles {

    private Profiles() {}

    /** Το αποτέλεσμα των κοινών βημάτων: κλιμάκωση, ομαδοποίηση, προβολή. */
    public record Clustering(List<String> features, double[][] scaled, KMeans.Result result,
                             double silhouette, Pca.Projection projection,
                             List<Map<String, Double>> centroidsInRealUnits) {}

    /**
     * Κλιμάκωση → k-means με επιλογή k από silhouette → προβολή PCA για το διάγραμμα.
     *
     * @param scaler ROBUST όταν περιμένουμε ακραίες τιμές (τηλεμετρία), Z_SCORE αλλιώς
     */
    public static Clustering cluster(DataProfile.Input input, List<String> features,
                                     Scaler scaler, int minK, int maxK) {
        double[][] raw = input.matrix(features);
        double[][] scaled = scaler.fitTransform(raw);
        Silhouette.Best best = Silhouette.chooseK(scaled, minK, maxK);
        Pca.Projection projection = Pca.project(scaled);

        // Τα κέντρα επιστρέφουν σε πραγματικές μονάδες: ο μέσος όρος των σημείων κάθε ομάδας.
        List<Map<String, Double>> centroids = new ArrayList<>();
        int k = best.result().k();
        int[] sizes = best.result().sizes();
        for (int c = 0; c < k; c++) {
            Map<String, Double> centroid = new LinkedHashMap<>();
            for (int f = 0; f < features.size(); f++) {
                double sum = 0;
                for (int i = 0; i < raw.length; i++) {
                    if (best.result().assignments()[i] == c) {
                        sum += raw[i][f];
                    }
                }
                centroid.put(features.get(f), sizes[c] == 0 ? 0.0 : round(sum / sizes[c]));
            }
            centroids.add(centroid);
        }
        return new Clustering(features, scaled, best.result(), best.score(), projection, centroids);
    }

    /**
     * Ανωμαλίες με ανθεκτικό z-score ανά πεδίο: διάμεσος και MAD αντί για μέσο όρο και
     * τυπική απόκλιση, γιατί μια ακραία τιμή θα παρέσυρε τον μέσο όρο και θα έκρυβε τον εαυτό της.
     */
    public static List<Analysis.Anomaly> robustAnomalies(DataProfile.Input input, List<String> fields,
                                                         double threshold, int maxPerField) {
        return robustAnomalies(input, fields, threshold, maxPerField, false);
    }

    /**
     * @param usePeaks true για να εξεταστεί η μέγιστη τιμή του κουβά αντί του μέσου όρου·
     *                 απαραίτητο για μεγέθη που εμφανίζονται ως στιγμιαίες αιχμές
     */
    public static List<Analysis.Anomaly> robustAnomalies(DataProfile.Input input, List<String> fields,
                                                         double threshold, int maxPerField,
                                                         boolean usePeaks) {
        List<Analysis.Anomaly> anomalies = new ArrayList<>();
        for (String field : fields) {
            double[] column = usePeaks ? input.peakColumn(field) : input.column(field);
            double median = Stats.median(column);
            double mad = Stats.mad(column);
            if (!Double.isFinite(mad) || mad < 1e-9) {
                continue; // σταθερό πεδίο: δεν υπάρχει "παράξενο"
            }
            List<Analysis.Anomaly> forField = new ArrayList<>();
            for (int i = 0; i < column.length; i++) {
                if (!Double.isFinite(column[i])) {
                    continue; // δεν μετρήθηκε
                }
                double z = Stats.robustZ(column[i], median, mad);
                if (Math.abs(z) >= threshold) {
                    forField.add(new Analysis.Anomaly(input.times().get(i), field, round(column[i]),
                            round(median), round(Math.abs(z)),
                            (z > 0 ? "πολύ ψηλά" : "πολύ χαμηλά") + " για το συνηθισμένο εύρος"
                                    + (usePeaks ? " (αιχμή μέσα στον κουβά)" : "")));
                }
            }
            forField.sort((a, b) -> Double.compare(b.score(), a.score()));
            anomalies.addAll(forField.subList(0, Math.min(maxPerField, forField.size())));
        }
        anomalies.sort((a, b) -> Double.compare(b.score(), a.score()));
        return anomalies;
    }

    /** Πόσο μακριά από το κέντρο της ομάδας του είναι κάθε σημείο, σε [0, ∞). */
    public static double[] centroidDistances(Clustering clustering) {
        double[][] scaled = clustering.scaled();
        double[] distances = new double[scaled.length];
        for (int i = 0; i < scaled.length; i++) {
            int cluster = clustering.result().assignments()[i];
            distances[i] = KMeans.distance(scaled[i], clustering.result().centroids()[cluster]);
        }
        return distances;
    }

    /**
     * Εξομαλύνει την κατάταξη με πλειοψηφία σε μικρό παράθυρο.
     * <p>
     * Όταν μια τιμή ταλαντώνεται γύρω από ένα όριο, η κατηγορία αλλάζει σε κάθε κουβά και η
     * λωρίδα του χρόνου γίνεται κομφετί: οπτικά αδιάβαστη και αναλυτικά παραπλανητική, αφού
     * δείχνει δεκάδες «μεταβάσεις» που δεν συνέβησαν. Η πλειοψηφία γειτονιάς κρατά το
     * επικρατούν καθεστώς και σβήνει τα μεμονωμένα αναπηδήματα, χωρίς να μετακινεί τα
     * πραγματικά σύνορα.
     *
     * ΠΡΟΣΟΧΗ: οι κατηγορίες συναγερμού ΔΕΝ εξομαλύνονται. Μια υποβάθμιση δύο κουβάδων είναι
     * ακριβώς το σήμα που ψάχνουμε, και μια πλειοψηφία γειτόνων θα την έσβηνε — το βρήκε το
     * test {@code telemetryFlagsSlowWithoutLoadAsDegradation}.
     *
     * @param window    περιττός αριθμός κουβάδων· 1 σημαίνει καμία εξομάλυνση
     * @param protectedClasses κατηγορίες που ούτε σβήνονται ούτε απλώνονται στους γείτονες
     */
    public static List<String> smooth(List<String> classNames, int window,
                                      java.util.Set<String> protectedClasses) {
        if (window < 3 || classNames.size() < window) {
            return classNames;
        }
        int half = window / 2;
        List<String> out = new ArrayList<>(classNames.size());
        for (int i = 0; i < classNames.size(); i++) {
            if (protectedClasses.contains(classNames.get(i))) {
                out.add(classNames.get(i));
                continue;
            }
            Map<String, Integer> votes = new LinkedHashMap<>();
            for (int j = Math.max(0, i - half); j <= Math.min(classNames.size() - 1, i + half); j++) {
                String neighbour = classNames.get(j);
                if (!protectedClasses.contains(neighbour)) {
                    votes.merge(neighbour, 1, Integer::sum);
                }
            }
            // Ισοψηφία: κερδίζει η τρέχουσα τιμή, για να μην εισάγουμε αυθαίρετες μεταβάσεις.
            String winner = classNames.get(i);
            int best = votes.getOrDefault(winner, 0);
            for (Map.Entry<String, Integer> vote : votes.entrySet()) {
                if (vote.getValue() > best) {
                    best = vote.getValue();
                    winner = vote.getKey();
                }
            }
            out.add(winner);
        }
        return out;
    }

    public static List<String> smooth(List<String> classNames, int window) {
        return smooth(classNames, window, java.util.Set.of());
    }

    public static List<Analysis.ClassCount> countClasses(List<String> classNames) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String name : classNames) {
            counts.merge(name, 1, Integer::sum);
        }
        int total = Math.max(1, classNames.size());
        return counts.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                .map(e -> new Analysis.ClassCount(e.getKey(), e.getValue(), round(100.0 * e.getValue() / total)))
                .toList();
    }

    public static List<Analysis.Assignment> assignments(DataProfile.Input input, Clustering clustering,
                                                        List<String> labels, List<String> classNames,
                                                        double[] anomalyScores) {
        List<Analysis.Assignment> out = new ArrayList<>(input.size());
        double[][] points = clustering.projection().points();
        for (int i = 0; i < input.size(); i++) {
            int cluster = clustering.result().assignments()[i];
            out.add(new Analysis.Assignment(
                    input.times().get(i), cluster,
                    labels.get(Math.min(cluster, labels.size() - 1)),
                    classNames.get(i),
                    round(points.length > i ? points[i][0] : 0),
                    round(points.length > i ? points[i][1] : 0),
                    round(anomalyScores[i])));
        }
        return out;
    }

    public static List<Analysis.Cluster> clusters(Clustering clustering, List<String> labels,
                                                  List<String> headlines) {
        List<Analysis.Cluster> out = new ArrayList<>();
        int[] sizes = clustering.result().sizes();
        int total = Math.max(1, clustering.result().assignments().length);
        for (int c = 0; c < clustering.result().k(); c++) {
            out.add(new Analysis.Cluster(c,
                    labels.get(Math.min(c, labels.size() - 1)),
                    sizes[c], round(100.0 * sizes[c] / total),
                    clustering.centroidsInRealUnits().get(c),
                    headlines.get(Math.min(c, headlines.size() - 1))));
        }
        return out;
    }

    public static Analysis empty(DataProfile profile, DataProfile.Input input, String why) {
        return new Analysis(input.source(), profile.id(), profile.title(), input.bucket(),
                input.from(), input.to(), input.size(), List.of(), "—",
                List.of(), List.of(), List.of(), List.of(),
                new Analysis.Verdict(0, "NO_DATA", why, List.of()), 0, 0);
    }

    public static double round(double v) {
        return Double.isFinite(v) ? Math.round(v * 1000.0) / 1000.0 : 0.0;
    }

    public static Instant at(DataProfile.Input input, int index) {
        return input.times().get(index);
    }
}
