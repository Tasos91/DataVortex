package com.thesis.datavortex.analytics.profile;

import com.thesis.datavortex.analytics.ml.Scaler;
import com.thesis.datavortex.analytics.ml.Stats;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Τηλεμετρία υπολογιστικών πόρων (AI/ML, υποδομή).
 * <p>
 * Η ιδιαιτερότητα αυτών των δεδομένων: οι μετρήσεις <b>συνυπάρχουν σε καθεστώτα</b> — αδράνεια,
 * κανονικό φορτίο, κορεσμός — και το ενδιαφέρον δεν είναι η απόλυτη τιμή αλλά ο <b>συνδυασμός</b>.
 * Μια καθυστέρηση 200ms με χρήση 95% είναι αναμενόμενη· η ίδια καθυστέρηση με χρήση 10% είναι
 * βλάβη. Γι' αυτό εδώ:
 * <ul>
 *   <li>ομαδοποιούμε σε καθεστώτα με k-means και ανθεκτική κλιμάκωση (οι αιχμές είναι κανόνας),</li>
 *   <li>κατατάσσουμε με κανόνες πάνω σε ποσοστημόρια, ώστε το "ψηλά" να ορίζεται από τα ίδια
 *       τα δεδομένα και όχι από σταθερά νούμερα που δεν ισχύουν για κάθε μηχάνημα,</li>
 *   <li>και ψάχνουμε ρητά τον συνδυασμό "αργό χωρίς φορτίο", που κανένα z-score δεν θα έβρισκε.</li>
 * </ul>
 */
@Component
public class AiTelemetryProfile implements DataProfile {

    private static final double ANOMALY_THRESHOLD = 3.5;
    /** Πόσες ανθεκτικές τυπικές αποκλίσεις πάνω από τη διάμεσο θεωρείται "υψηλή" καθυστέρηση. */
    private static final double LATENCY_Z = 2.5;

    /** Παράθυρο εξομάλυνσης της κατάταξης· σβήνει τα μεμονωμένα αναπηδήματα. */
    private static final int SMOOTHING = 5;
    /** Η κατηγορία-συναγερμός: δεν εξομαλύνεται ποτέ, όσο σύντομη κι αν είναι. */
    private static final String DEGRADED = "υποβάθμιση";

    @Override
    public String id() {
        return "ai-telemetry";
    }

    @Override
    public String title() {
        return "Τηλεμετρία υπολογιστικών πόρων";
    }

    @Override
    public String approach() {
        return "k-means σε καθεστώτα φορτίου, κατάταξη με ποσοστημόρια, και ρητός έλεγχος για "
                + "υψηλή καθυστέρηση χωρίς φορτίο";
    }

    @Override
    public int score(String sourceId, Map<String, FieldRoles> roles) {
        boolean hasCore = roles.containsValue(FieldRoles.UTILIZATION) || roles.containsValue(FieldRoles.LATENCY);
        if (!hasCore) {
            return 0;
        }
        int score = 2 * FieldRoles.countOf(roles, FieldRoles.UTILIZATION, FieldRoles.LATENCY,
                FieldRoles.THROUGHPUT, FieldRoles.MEMORY, FieldRoles.ERRORS, FieldRoles.TEMPERATURE);
        String name = sourceId.toLowerCase();
        if (name.contains("telemetry") || name.contains("gpu") || name.contains("infra")
                || name.contains("host") || name.contains("ai") || name.contains("ml")) {
            score += 3;
        }
        return score;
    }

    @Override
    public Analysis analyse(Input input) {
        if (input.size() < 4 || input.fields().isEmpty()) {
            return Profiles.empty(this, input, "Πολύ λίγα σημεία για ανάλυση σε αυτό το διάστημα.");
        }
        Map<String, FieldRoles> roles = FieldRoles.of(input.fields());
        List<String> features = input.fields();

        // Ανθεκτική κλιμάκωση: στην τηλεμετρία οι αιχμές είναι κανονικές, και ο μέσος όρος
        // με την τυπική απόκλιση θα παρασύρονταν από αυτές.
        Profiles.Clustering clustering = Profiles.cluster(input, features, Scaler.ROBUST, 2, 6);

        // Οι ομάδες παίρνουν ονόματα κατά σειρά φορτίου, όχι κατά τον αριθμό που έτυχε στο k-means.
        List<String> loadOrderedLabels = labelByLoad(clustering, roles);

        double[] distances = Profiles.centroidDistances(clustering);
        List<String> classes = Profiles.smooth(
                classify(input, roles, clustering, loadOrderedLabels), SMOOTHING, Set.of(DEGRADED));
        double[] anomalyScores = anomalyScores(input, features, distances);

        // Για καθυστέρηση και σφάλματα κοιτάμε τις ΑΙΧΜΕΣ του κουβά: μια αργή μέτρηση μέσα σε
        // ογδόντα γρήγορες δεν αλλάζει τον μέσο όρο, αλλά είναι ακριβώς αυτό που ψάχνουμε.
        List<String> spikyFields = features.stream()
                .filter(f -> FieldRoles.of(f) == FieldRoles.LATENCY || FieldRoles.of(f) == FieldRoles.ERRORS)
                .toList();
        List<String> smoothFields = features.stream().filter(f -> !spikyFields.contains(f)).toList();

        List<Analysis.Anomaly> anomalies = new ArrayList<>(
                Profiles.robustAnomalies(input, smoothFields, ANOMALY_THRESHOLD, 3, false));
        anomalies.addAll(Profiles.robustAnomalies(input, spikyFields, ANOMALY_THRESHOLD, 5, true));
        anomalies.addAll(slowWithoutLoad(input, roles));
        anomalies.sort(Comparator.comparingDouble(Analysis.Anomaly::score).reversed());
        List<Analysis.Anomaly> topAnomalies = anomalies.subList(0, Math.min(25, anomalies.size()));

        List<Analysis.ClassCount> classCounts = Profiles.countClasses(classes);
        return new Analysis(input.source(), id(), title(), input.bucket(), input.from(), input.to(),
                input.size(), features,
                "k-means (k=%d, silhouette %.2f) + κανόνες ποσοστημορίων"
                        .formatted(clustering.result().k(), clustering.silhouette()),
                Profiles.clusters(clustering, loadOrderedLabels, headlines(clustering, roles)),
                Profiles.assignments(input, clustering, loadOrderedLabels, classes, anomalyScores),
                topAnomalies, classCounts,
                verdict(input, roles, classCounts, topAnomalies), 
                Profiles.round(clustering.projection().explainedX() * 100),
                Profiles.round(clustering.projection().explainedY() * 100));
    }

    /**
     * Ονόματα ανά πλήθος ομάδων, ώστε η κλίμακα να βγάζει νόημα: με δύο ομάδες δεν λέμε
     * «ελαφρύ φορτίο», και με τρεις η μεσαία είναι «κανονικό» και όχι «ακραίο».
     */
    private static final Map<Integer, List<String>> NAMES_BY_K = Map.of(
            1, List.of("ενιαία συμπεριφορά"),
            2, List.of("αδράνεια", "κορεσμός"),
            3, List.of("αδράνεια", "κανονικό φορτίο", "κορεσμός"),
            4, List.of("αδράνεια", "ελαφρύ φορτίο", "βαρύ φορτίο", "κορεσμός"),
            5, List.of("αδράνεια", "ελαφρύ φορτίο", "κανονικό φορτίο", "βαρύ φορτίο", "κορεσμός"),
            6, List.of("αδράνεια", "ελαφρύ φορτίο", "κανονικό φορτίο", "βαρύ φορτίο", "κορεσμός", "ακραίο"));

    /** Δείκτης φορτίου ανά ομάδα: μέσος όρος των πεδίων που δείχνουν «πόσο δουλεύει». */
    private List<String> labelByLoad(Profiles.Clustering clustering, Map<String, FieldRoles> roles) {
        int k = clustering.result().k();
        List<String> names = NAMES_BY_K.getOrDefault(k, NAMES_BY_K.get(6));
        List<Integer> order = new ArrayList<>();
        for (int c = 0; c < k; c++) {
            order.add(c);
        }
        // Κανονικοποίηση ανά πεδίο πριν τη σύγκριση: αλλιώς ένα πεδίο σε χιλιάδες (throughput)
        // καθορίζει μόνο του τη σειρά και η ομάδα με χρήση 8% μπορεί να βγει "πιο φορτωμένη"
        // από εκείνη με 96%.
        Map<String, double[]> spans = new java.util.HashMap<>();
        for (String field : clustering.features()) {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            for (Map<String, Double> centroid : clustering.centroidsInRealUnits()) {
                double v = centroid.getOrDefault(field, 0.0);
                min = Math.min(min, v);
                max = Math.max(max, v);
            }
            spans.put(field, new double[]{min, max});
        }
        order.sort(Comparator.comparingDouble(
                c -> loadIndex(clustering.centroidsInRealUnits().get(c), roles, spans)));

        String[] labels = new String[k];
        for (int rank = 0; rank < k; rank++) {
            labels[order.get(rank)] = names.get(Math.min(rank, names.size() - 1));
        }
        return List.of(labels);
    }

    private static double loadIndex(Map<String, Double> centroid, Map<String, FieldRoles> roles,
                                    Map<String, double[]> spans) {
        double sum = 0;
        int count = 0;
        for (Map.Entry<String, Double> e : centroid.entrySet()) {
            FieldRoles role = roles.getOrDefault(e.getKey(), FieldRoles.OTHER);
            if (role == FieldRoles.UTILIZATION || role == FieldRoles.MEMORY
                    || role == FieldRoles.THROUGHPUT || role == FieldRoles.TEMPERATURE) {
                sum += normalised(e.getKey(), e.getValue(), spans);
                count++;
            }
        }
        // Χωρίς πεδία φορτίου, πέφτουμε στην καθυστέρηση: κι αυτή ανεβαίνει με το φορτίο.
        if (count == 0) {
            for (Map.Entry<String, Double> e : centroid.entrySet()) {
                if (roles.getOrDefault(e.getKey(), FieldRoles.OTHER) == FieldRoles.LATENCY) {
                    sum += normalised(e.getKey(), e.getValue(), spans);
                    count++;
                }
            }
        }
        return count == 0 ? 0 : sum / count;
    }

    private static double normalised(String field, double value, Map<String, double[]> spans) {
        double[] span = spans.get(field);
        if (span == null || span[1] - span[0] < 1e-9) {
            return 0.5;
        }
        return (value - span[0]) / (span[1] - span[0]);
    }

    private List<String> headlines(Profiles.Clustering clustering, Map<String, FieldRoles> roles) {
        List<String> out = new ArrayList<>();
        for (Map<String, Double> centroid : clustering.centroidsInRealUnits()) {
            String utilisation = FieldRoles.first(roles, FieldRoles.UTILIZATION).orElse(null);
            String latency = FieldRoles.first(roles, FieldRoles.LATENCY).orElse(null);
            StringBuilder sb = new StringBuilder();
            if (utilisation != null) {
                sb.append("χρήση ").append(centroid.get(utilisation));
            }
            if (latency != null) {
                sb.append(sb.isEmpty() ? "" : ", ").append("καθυστέρηση ").append(centroid.get(latency));
            }
            if (sb.isEmpty()) {
                sb.append(centroid.entrySet().stream().findFirst()
                        .map(e -> e.getKey() + " " + e.getValue()).orElse("—"));
            }
            out.add(sb.toString());
        }
        return out;
    }

    /**
     * Η κατάταξη πατά στο <b>καθεστώς</b> του κουβά, και οι κανόνες μπαίνουν από πάνω.
     * <p>
     * Η προηγούμενη εκδοχή έβγαζε την κατηγορία αποκλειστικά από ποσοστημόρια. Το πρόβλημα:
     * όταν μια τιμή ταλαντώνεται γύρω από το 60ό ποσοστημόριο, η κατηγορία αλλάζει σε κάθε
     * κουβά — 114 «μεταβάσεις» σε 608 κουβάδες, που δεν αντιστοιχούν σε τίποτα πραγματικό.
     * Η ομαδοποίηση είναι εξ ορισμού σταθερή (κοιτάζει όλα τα πεδία μαζί), οπότε δίνει τη
     * βάση· οι κανόνες κρατιούνται για ό,τι η ομαδοποίηση δεν μπορεί να δει, δηλαδή τον
     * <b>συνδυασμό</b> «αργό χωρίς φορτίο».
     */
    private List<String> classify(Input input, Map<String, FieldRoles> roles,
                                  Profiles.Clustering clustering, List<String> labels) {
        Optional<String> utilisation = FieldRoles.first(roles, FieldRoles.UTILIZATION);
        Optional<String> latency = FieldRoles.first(roles, FieldRoles.LATENCY);

        double[] util = utilisation.map(input::column).orElse(null);
        double[] lat = latency.map(input::peakColumn).orElse(null);
        double utilMedian = util == null ? 0 : Stats.median(util);
        double latMedian = lat == null ? 0 : Stats.median(lat);
        double latMad = lat == null ? 0 : Stats.mad(lat);

        int[] assignments = clustering.result().assignments();
        List<String> classes = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            // ΠΡΟΣΗΜΑΣΜΕΝΟ z, όχι απόλυτη τιμή: υποβάθμιση είναι μόνο η ΥΨΗΛΗ καθυστέρηση.
            // Με απόλυτη τιμή, μια ασυνήθιστα ΧΑΜΗΛΗ καθυστέρηση σε ήρεμη ώρα χαρακτηριζόταν
            // κι αυτή "υποβάθμιση" — 14% του χρόνου σε δεδομένα με 0,4% πραγματικές ανωμαλίες.
            double latZ = lat == null ? 0 : Stats.robustZ(lat[i], latMedian, latMad);
            boolean quiet = util == null || !Double.isFinite(util[i]) || util[i] <= utilMedian;
            if (lat != null && latZ >= LATENCY_Z && quiet) {
                // Το σημαντικό εύρημα: αργό ενώ δεν δουλεύει. Δείχνει βλάβη, όχι φόρτο.
                classes.add(DEGRADED);
            } else {
                int cluster = i < assignments.length ? assignments[i] : 0;
                classes.add(labels.get(Math.min(cluster, labels.size() - 1)));
            }
        }
        return classes;
    }

    /**
     * Έχει το πεδίο αρκετή διασπορά για να βγάλουν νόημα τα ποσοστημόρια;
     * Κριτήριο: το εύρος 10%–90% να είναι τουλάχιστον 15% του επιπέδου.
     */
    private static boolean varies(double[] column) {
        if (column == null || column.length < 4) {
            return false;
        }
        double level = Math.max(Math.abs(Stats.median(column)), 1e-9);
        double spread = Stats.quantile(column, 0.90) - Stats.quantile(column, 0.10);
        return spread / level >= 0.15;
    }

    private List<Analysis.Anomaly> slowWithoutLoad(Input input, Map<String, FieldRoles> roles) {
        Optional<String> utilisation = FieldRoles.first(roles, FieldRoles.UTILIZATION);
        Optional<String> latency = FieldRoles.first(roles, FieldRoles.LATENCY);
        if (utilisation.isEmpty() || latency.isEmpty()) {
            return List.of();
        }
        double[] util = input.column(utilisation.get());
        double[] lat = input.peakColumn(latency.get()); // αιχμή καθυστέρησης, όχι μέσος όρος
        double latMedian = Stats.median(lat);
        double latMad = Stats.mad(lat);
        double utilMedian = Stats.median(util);

        List<Analysis.Anomaly> found = new ArrayList<>();
        for (int i = 0; i < lat.length; i++) {
            double z = Stats.robustZ(lat[i], latMedian, latMad);
            if (z >= 2.5 && util[i] <= utilMedian) {
                found.add(new Analysis.Anomaly(Profiles.at(input, i), latency.get(),
                        Profiles.round(lat[i]), Profiles.round(latMedian), Profiles.round(z),
                        "υψηλή καθυστέρηση με χαμηλή χρήση (" + Profiles.round(util[i]) + ")"));
            }
        }
        return found;
    }

    private double[] anomalyScores(Input input, List<String> features, double[] distances) {
        // Συνδυασμός: πόσο μακριά από το κέντρο της ομάδας + πόσο ακραία η χειρότερη μέτρηση.
        //
        // ΠΡΟΣΟΧΗ: διάμεσος και MAD κάθε πεδίου υπολογίζονται ΜΙΑ φορά, έξω από τον βρόχο των
        // σημείων. Μέσα στον βρόχο ήταν δύο ταξινομήσεις ανά σημείο ανά πεδίο — O(n² log n) —
        // και κόστιζαν 250ms από τα 385ms της ανάλυσης, για δουλειά που γίνεται σε 1ms.
        double[] scores = new double[input.size()];
        double distanceMedian = Stats.median(distances);
        double distanceMad = Stats.mad(distances);

        int fieldCount = features.size();
        double[][] columns = new double[fieldCount][];
        double[] medians = new double[fieldCount];
        double[] mads = new double[fieldCount];
        for (int f = 0; f < fieldCount; f++) {
            columns[f] = input.column(features.get(f));
            medians[f] = Stats.median(columns[f]);
            mads[f] = Stats.mad(columns[f]);
        }

        for (int i = 0; i < input.size(); i++) {
            double worst = Math.abs(Stats.robustZ(distances[i], distanceMedian, distanceMad));
            for (int f = 0; f < fieldCount; f++) {
                worst = Math.max(worst, Math.abs(Stats.robustZ(columns[f][i], medians[f], mads[f])));
            }
            scores[i] = worst;
        }
        return scores;
    }

    private Analysis.Verdict verdict(Input input, Map<String, FieldRoles> roles,
                                     List<Analysis.ClassCount> classes, List<Analysis.Anomaly> anomalies) {
        double saturated = share(classes, "κορεσμός");
        double degraded = share(classes, DEGRADED);
        double idle = share(classes, "αδράνεια");
        double anomalyRate = 100.0 * anomalies.size() / Math.max(1, input.size());

        int score = (int) Math.round(100
                - degraded * 1.2      // η υποβάθμιση είναι το χειρότερο σημάδι
                - saturated * 0.5
                - Math.min(30, anomalyRate * 2));
        score = Math.max(0, Math.min(100, score));

        List<String> findings = new ArrayList<>();
        if (degraded > 2) {
            findings.add("Στο %.0f%% των κουβάδων υπάρχει υψηλή καθυστέρηση ΧΩΡΙΣ αντίστοιχο φορτίο — αυτό δεν εξηγείται από τη ζήτηση."
                    .formatted(degraded));
        }
        if (saturated > 25) {
            findings.add("Κορεσμός στο %.0f%% του διαστήματος: ο πόρος δουλεύει στα όριά του."
                    .formatted(saturated));
        }
        if (idle > 60) {
            findings.add("Αδράνεια στο %.0f%% του διαστήματος: υπάρχει περιθώριο για πιο πυκνή χρήση."
                    .formatted(idle));
        }
        FieldRoles.first(roles, FieldRoles.LATENCY).ifPresent(field -> {
            double trend = Stats.trendPerSample(input.column(field)) * input.size() * 100;
            if (Math.abs(trend) > 15) {
                findings.add("Η %s %s κατά %.0f%% μέσα στο διάστημα."
                        .formatted(field, trend > 0 ? "ανεβαίνει" : "πέφτει", Math.abs(trend)));
            }
        });
        if (!anomalies.isEmpty()) {
            findings.add("%d ανωμαλίες, η εντονότερη στο %s (%s)."
                    .formatted(anomalies.size(), anomalies.get(0).field(), anomalies.get(0).reason()));
        }
        if (findings.isEmpty()) {
            findings.add("Καμία αξιοσημείωτη απόκλιση στο διάστημα.");
        }

        String status = score >= 80 ? "OK" : score >= 55 ? "WATCH" : "ALERT";
        String headline = switch (status) {
            case "OK" -> "Σταθερή λειτουργία";
            case "WATCH" -> "Αξίζει προσοχή";
            default -> "Χρειάζεται παρέμβαση";
        };
        return new Analysis.Verdict(score, status, headline, findings);
    }

    private static double share(List<Analysis.ClassCount> classes, String name) {
        return classes.stream().filter(c -> c.className().equals(name))
                .mapToDouble(Analysis.ClassCount::share).findFirst().orElse(0);
    }
}
