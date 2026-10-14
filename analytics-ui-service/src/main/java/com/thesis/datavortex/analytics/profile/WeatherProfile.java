package com.thesis.datavortex.analytics.profile;

import com.thesis.datavortex.analytics.ml.Scaler;
import com.thesis.datavortex.analytics.ml.Stats;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Μετεωρολογικά δεδομένα.
 * <p>
 * Εδώ η ανάλυση είναι <b>θεμελιωδώς διαφορετική</b> από την τηλεμετρία, για δύο λόγους:
 * <ol>
 *   <li><b>Εποχικότητα.</b> Τα δεδομένα έχουν ημερήσιο κύκλο. Ένα ανθεκτικό z-score πάνω σε όλο
 *       το διάστημα θα σήμαινε κάθε μεσημέρι ως «ζεστό» και καμία νύχτα, που είναι άχρηστο.
 *       Γι' αυτό η βάση σύγκρισης είναι <b>η ίδια ώρα της ημέρας</b>: μια ζεστή νύχτα είναι
 *       ανωμαλία, ένα ζεστό μεσημέρι δεν είναι.</li>
 *   <li><b>Οι κατηγορίες είναι γνωστές εκ των προτέρων.</b> Βροχή, καύσωνας, ψύχος και άνεμος
 *       ορίζονται από τη μετεωρολογία και όχι από τα δεδομένα· θα ήταν λάθος να τις
 *       «ανακαλύψει» ένας αλγόριθμος. Οι ομάδες του k-means χρησιμεύουν για να δούμε ποια
 *       <i>καθεστώτα καιρού</i> συνυπάρχουν, και ονομάζονται από τις τιμές τους.</li>
 * </ol>
 */
@Component
public class WeatherProfile implements DataProfile {

    private static final double SEASONAL_THRESHOLD = 3.0;
    /** Μέση ένταση βροχής ανά μέτρηση, πάνω από την οποία ο κουβάς θεωρείται βροχερός. */
    private static final double RAIN_INTENSITY = 0.1;
    /** Μέση ταχύτητα ανέμου (km/h) για να χαρακτηριστεί ανεμώδης ο κουβάς. */
    private static final double WINDY_AVG = 25;
    /** Κάτω από τόσα δείγματα, μια ώρα της ημέρας δεν φτιάχνει βάση σύγκρισης. */
    private static final int MIN_SAMPLES_PER_HOUR = 3;

    /** Παράθυρο εξομάλυνσης της κατάταξης· σβήνει τα μεμονωμένα αναπηδήματα. */
    private static final int SMOOTHING = 5;

    @Override
    public String id() {
        return "weather";
    }

    @Override
    public String title() {
        return "Μετεωρολογικά δεδομένα";
    }

    @Override
    public String approach() {
        return "εποχική βάση σύγκρισης ανά ώρα της ημέρας, μετεωρολογικές κατηγορίες με κανόνες, "
                + "και k-means για τα καθεστώτα καιρού";
    }

    @Override
    public int score(String sourceId, Map<String, FieldRoles> roles) {
        if (!roles.containsValue(FieldRoles.TEMPERATURE)) {
            return 0;
        }
        int companions = FieldRoles.countOf(roles, FieldRoles.HUMIDITY, FieldRoles.PRESSURE,
                FieldRoles.WIND, FieldRoles.RAIN);
        if (companions == 0) {
            return 0; // μόνο θερμοκρασία: μπορεί να είναι και τηλεμετρία μηχανήματος
        }
        int score = 3 * companions + 2;
        String name = sourceId.toLowerCase(java.util.Locale.ROOT);
        if (name.contains("weather") || name.contains("meteo") || name.contains("climate")
                || name.contains("station") || name.contains("kair")
                || name.contains("καιρ") || name.contains("μετεωρ") || name.contains("σταθμ")) {
            score += 4;
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

        // Z-score και όχι ανθεκτική κλιμάκωση: ο καιρός δεν έχει αιχμές τηλεμετρίας, και
        // θέλουμε οι ομαλές μεταβολές να μετρήσουν κανονικά.
        Profiles.Clustering clustering = Profiles.cluster(input, features, Scaler.Z_SCORE, 2, 5);

        List<String> labels = labelRegimes(clustering, roles);
        // Οι ακραίες συνθήκες δεν εξομαλύνονται: μια σύντομη καταιγίδα είναι γεγονός, όχι θόρυβος.
        List<String> classes = Profiles.smooth(classify(input, roles), SMOOTHING,
                java.util.Set.of("βροχή", "καύσωνας", "ψύχος"));
        List<Analysis.Anomaly> anomalies = seasonalAnomalies(input, features);
        double[] scores = seasonalScores(input, features);

        List<Analysis.ClassCount> classCounts = Profiles.countClasses(classes);
        return new Analysis(input.source(), id(), title(), input.bucket(), input.from(), input.to(),
                input.size(), features,
                "k-means (k=%d, silhouette %.2f) + εποχική βάση ανά ώρα"
                        .formatted(clustering.result().k(), clustering.silhouette()),
                Profiles.clusters(clustering, labels, headlines(clustering, roles)),
                Profiles.assignments(input, clustering, labels, classes, scores),
                anomalies.subList(0, Math.min(25, anomalies.size())), classCounts,
                verdict(input, roles, classCounts, anomalies),
                Profiles.round(clustering.projection().explainedX() * 100),
                Profiles.round(clustering.projection().explainedY() * 100));
    }

    /** Τα καθεστώτα ονομάζονται από τις τιμές τους, όχι από τη σειρά που βγήκαν. */
    private List<String> labelRegimes(Profiles.Clustering clustering, Map<String, FieldRoles> roles) {
        String temp = FieldRoles.first(roles, FieldRoles.TEMPERATURE).orElse(null);
        String rain = FieldRoles.first(roles, FieldRoles.RAIN).orElse(null);
        String humidity = FieldRoles.first(roles, FieldRoles.HUMIDITY).orElse(null);
        String wind = FieldRoles.first(roles, FieldRoles.WIND).orElse(null);

        List<Map<String, Double>> centroids = clustering.centroidsInRealUnits();
        double[] temps = centroids.stream().mapToDouble(c -> value(c, temp)).toArray();
        double warm = temps.length > 0 ? Stats.quantile(temps, 0.66) : 0;
        double cool = temps.length > 0 ? Stats.quantile(temps, 0.33) : 0;

        List<String> labels = new ArrayList<>();
        for (Map<String, Double> centroid : centroids) {
            double t = value(centroid, temp);
            double r = value(centroid, rain);
            double h = value(centroid, humidity);
            double w = value(centroid, wind);
            if (rain != null && r >= RAIN_INTENSITY) {
                labels.add("βροχερό");
            } else if (wind != null && w > WINDY_AVG) {
                labels.add("ανεμώδες");
            } else if (t >= warm && (humidity == null || h < 55)) {
                labels.add("ζεστό και ξηρό");
            } else if (t >= warm) {
                labels.add("ζεστό και υγρό");
            } else if (t <= cool) {
                labels.add("ψυχρό");
            } else {
                labels.add("ήπιο");
            }
        }
        return dedupe(labels);
    }

    /** Δύο ομάδες με το ίδιο όνομα μπερδεύουν· προσθέτουμε αρίθμηση μόνο όπου χρειάζεται. */
    private static List<String> dedupe(List<String> labels) {
        Map<String, Integer> seen = new HashMap<>();
        for (String label : labels) {
            seen.merge(label, 1, Integer::sum);
        }
        Map<String, Integer> used = new HashMap<>();
        List<String> out = new ArrayList<>();
        for (String label : labels) {
            if (seen.get(label) > 1) {
                int index = used.merge(label, 1, Integer::sum);
                out.add(label + " " + index);
            } else {
                out.add(label);
            }
        }
        return out;
    }

    private List<String> headlines(Profiles.Clustering clustering, Map<String, FieldRoles> roles) {
        String temp = FieldRoles.first(roles, FieldRoles.TEMPERATURE).orElse(null);
        String humidity = FieldRoles.first(roles, FieldRoles.HUMIDITY).orElse(null);
        List<String> out = new ArrayList<>();
        for (Map<String, Double> centroid : clustering.centroidsInRealUnits()) {
            StringBuilder sb = new StringBuilder();
            if (temp != null) {
                sb.append(value(centroid, temp)).append("°");
            }
            if (humidity != null) {
                sb.append(sb.isEmpty() ? "" : ", ").append("υγρασία ").append(value(centroid, humidity)).append("%");
            }
            out.add(sb.isEmpty() ? "—" : sb.toString());
        }
        return out;
    }

    /** Μετεωρολογικές κατηγορίες: ορίζονται από τη φύση του φαινομένου, όχι από τα δεδομένα. */
    private List<String> classify(Input input, Map<String, FieldRoles> roles) {
        double[] temp = column(input, roles, FieldRoles.TEMPERATURE);
        // ΠΡΟΣΟΧΗ: τα κατώφλια εδώ είναι απόλυτα (μετεωρολογικά), άρα πρέπει να συγκρίνονται με
        // ΜΕΣΟΥΣ ΟΡΟΥΣ. Η μέγιστη τιμή ενός κουβά με εκατοντάδες μετρήσεις ξεπερνά σχεδόν πάντα
        // οποιοδήποτε κατώφλι — "έπεσε μια στάλα" ή "φύσηξε μια ριπή" δεν κάνουν βροχερή ή
        // ανεμώδη ώρα. Οι αιχμές χρησιμεύουν στις ανωμαλίες, όπου η βάση σύγκρισης είναι
        // επίσης αιχμή, άρα συγκρίνουμε όμοια με όμοια.
        double[] rain = column(input, roles, FieldRoles.RAIN);
        double[] wind = column(input, roles, FieldRoles.WIND);
        double[] humidity = column(input, roles, FieldRoles.HUMIDITY);

        List<String> classes = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            double t = at(temp, i);
            if (rain != null && at(rain, i) >= RAIN_INTENSITY) {
                classes.add("βροχή");
            } else if (temp != null && t >= 35) {
                classes.add("καύσωνας");
            } else if (temp != null && t <= 5) {
                classes.add("ψύχος");
            } else if (wind != null && at(wind, i) >= WINDY_AVG) {
                classes.add("άνεμος");
            } else if (humidity != null && at(humidity, i) >= 85) {
                classes.add("υγρασία");
            } else {
                classes.add("ήπιο");
            }
        }
        return classes;
    }

    /**
     * Ανωμαλίες ως προς την ίδια ώρα της ημέρας.
     * <p>
     * Για κάθε ώρα φτιάχνουμε δική της διάμεσο και MAD, και συγκρίνουμε κάθε μέτρηση με
     * αυτές. Έτσι εντοπίζεται η «ζεστή νύχτα» ή η «κρύα μέρα», που μια καθολική σύγκριση
     * θα τις έκρυβε μέσα στον ημερήσιο κύκλο.
     */
    private List<Analysis.Anomaly> seasonalAnomalies(Input input, List<String> fields) {
        // Δύο περάσματα: στους μέσους όρους (παρατεταμένη απόκλιση, π.χ. ζεστή νύχτα) και στις
        // αιχμές (στιγμιαία, π.χ. χαλασμένος αισθητήρας ή ριπή). Κάθε πέρασμα συγκρίνεται με
        // βάση φτιαγμένη από το ΙΔΙΟ μέγεθος, αλλιώς θα σημαίναμε τα πάντα.
        Map<String, Analysis.Anomaly> best = new java.util.LinkedHashMap<>();
        collect(input, fields, false, best);
        collect(input, fields, true, best);

        List<Analysis.Anomaly> found = new ArrayList<>(best.values());
        found.sort(Comparator.comparingDouble(Analysis.Anomaly::score).reversed());
        return found;
    }

    private void collect(Input input, List<String> fields, boolean usePeaks,
                         Map<String, Analysis.Anomaly> best) {
        int[] hours = hoursOf(input);
        for (String field : fields) {
            double[] column = usePeaks ? input.peakColumn(field) : input.column(field);
            Baseline baseline = Baseline.of(column, hours);
            for (int i = 0; i < column.length; i++) {
                double[] stats = baseline.forHour(hours[i]);
                if (stats == null || stats[1] < 1e-9) {
                    continue;
                }
                double z = Stats.robustZ(column[i], stats[0], stats[1]);
                if (Math.abs(z) < SEASONAL_THRESHOLD) {
                    continue;
                }
                String reason = baseline.perHour()
                        ? "ασυνήθιστο για την ώρα %02d:00 (συνήθως %.1f)".formatted(hours[i], stats[0])
                        : "ασυνήθιστο για το διάστημα (συνήθως %.1f)".formatted(stats[0]);
                if (usePeaks) {
                    reason = "στιγμιαία αιχμή, " + reason;
                }
                Analysis.Anomaly anomaly = new Analysis.Anomaly(Profiles.at(input, i), field,
                        Profiles.round(column[i]), Profiles.round(stats[0]),
                        Profiles.round(Math.abs(z)), reason);
                // Ίδιο σημείο και πεδίο: κρατάμε την εντονότερη ένδειξη, όχι δύο γραμμές.
                best.merge(input.times().get(i) + "|" + field, anomaly,
                        (a, b) -> a.score() >= b.score() ? a : b);
            }
        }
    }

    /**
     * Βάση σύγκρισης ανά ώρα της ημέρας, με καθολικό fallback.
     * <p>
     * Με χονδρούς κουβάδες (π.χ. ωριαίους σε 36 ώρες) κάθε ώρα της ημέρας έχει ένα-δύο
     * δείγματα, που δεν φτιάχνουν βάση. Τότε χρησιμοποιείται η καθολική διάμεσος: χάνουμε την
     * εποχικότητα, αλλά δεν χάνουμε την ανίχνευση — και το μήνυμα το λέει ρητά.
     */
    private record Baseline(Map<Integer, double[]> byHour, double[] global, boolean perHour) {

        static Baseline of(double[] column, int[] hours) {
            Map<Integer, List<Double>> grouped = new HashMap<>();
            for (int i = 0; i < column.length; i++) {
                grouped.computeIfAbsent(hours[i], h -> new ArrayList<>()).add(column[i]);
            }
            Map<Integer, Double> centres = new HashMap<>();
            grouped.forEach((hour, values) -> {
                if (values.size() >= MIN_SAMPLES_PER_HOUR) {
                    centres.put(hour, Stats.median(values.stream().mapToDouble(Double::doubleValue).toArray()));
                }
            });

            // Η διασπορά ΜΕΣΑ σε μια ώρα δεν μπορεί να εκτιμηθεί από τρία δείγματα: αν ένα από
            // αυτά είναι ο ακραίος, το MAD βγαίνει 0 και ο ακραίος κρύβεται πίσω από τη δική
            // του επίδραση. Την εκτιμούμε συγκεντρωτικά, από τα υπόλοιπα ΟΛΩΝ των ωρών γύρω
            // από το δικό τους κέντρο — πολλά δείγματα, ίδια ποσότητα.
            List<Double> residuals = new ArrayList<>();
            for (int i = 0; i < column.length; i++) {
                Double centre = centres.get(hours[i]);
                if (centre != null) {
                    residuals.add(column[i] - centre);
                }
            }
            double pooled = residuals.size() >= MIN_SAMPLES_PER_HOUR
                    ? Stats.mad(residuals.stream().mapToDouble(Double::doubleValue).toArray())
                    : 0;

            Map<Integer, double[]> byHour = new HashMap<>();
            grouped.forEach((hour, values) -> {
                Double centre = centres.get(hour);
                if (centre == null) {
                    return;
                }
                double own = Stats.mad(values.stream().mapToDouble(Double::doubleValue).toArray());
                // Προτίμησε τη διασπορά της ώρας· αν δεν στέκει, τη συγκεντρωτική.
                double scale = own > 1e-9 ? own : pooled;
                byHour.put(hour, new double[]{centre, scale});
            });

            double[] global = {Stats.median(column), Stats.mad(column)};
            // Αν οι μισές ώρες δεν έχουν βάση, η εποχικότητα δεν στέκει για αυτά τα δεδομένα.
            boolean usable = byHour.size() >= Math.max(3, grouped.size() / 2);
            return new Baseline(byHour, global, usable);
        }

        double[] forHour(int hour) {
            if (perHour) {
                double[] stats = byHour.get(hour);
                return stats != null ? stats : global;
            }
            return global;
        }
    }

    private double[] seasonalScores(Input input, List<String> fields) {
        int[] hours = hoursOf(input);
        double[] scores = new double[input.size()];
        for (String field : fields) {
            for (boolean usePeaks : new boolean[]{false, true}) {
                double[] column = usePeaks ? input.peakColumn(field) : input.column(field);
                Baseline baseline = Baseline.of(column, hours);
                for (int i = 0; i < column.length; i++) {
                    double[] stats = baseline.forHour(hours[i]);
                    if (stats != null && stats[1] > 1e-9) {
                        scores[i] = Math.max(scores[i], Math.abs(Stats.robustZ(column[i], stats[0], stats[1])));
                    }
                }
            }
        }
        return scores;
    }

    private static int[] hoursOf(Input input) {
        int[] hours = new int[input.size()];
        for (int i = 0; i < hours.length; i++) {
            hours[i] = input.times().get(i).atZone(ZoneOffset.UTC).getHour();
        }
        return hours;
    }

    private Analysis.Verdict verdict(Input input, Map<String, FieldRoles> roles,
                                     List<Analysis.ClassCount> classes, List<Analysis.Anomaly> anomalies) {
        double extreme = share(classes, "καύσωνας") + share(classes, "ψύχος") + share(classes, "άνεμος");
        double rain = share(classes, "βροχή");
        double anomalyRate = 100.0 * anomalies.size() / Math.max(1, input.size());

        int score = (int) Math.round(100 - extreme * 1.5 - Math.min(20, anomalyRate));
        score = Math.max(0, Math.min(100, score));

        List<String> findings = new ArrayList<>();
        String dominant = classes.isEmpty() ? "—" : classes.get(0).className();
        findings.add("Επικρατεί «%s» στο %.0f%% του διαστήματος."
                .formatted(dominant, classes.isEmpty() ? 0 : classes.get(0).share()));
        if (rain > 10) {
            findings.add("Βροχή στο %.0f%% των κουβάδων.".formatted(rain));
        }
        if (extreme > 5) {
            findings.add("Ακραίες συνθήκες στο %.0f%% του διαστήματος.".formatted(extreme));
        }
        Optional<String> temp = FieldRoles.first(roles, FieldRoles.TEMPERATURE);
        temp.ifPresent(field -> {
            double trend = Stats.trendPerSample(input.column(field)) * input.size() * 100;
            if (Math.abs(trend) > 10) {
                findings.add("Η θερμοκρασία %s κατά %.0f%% μέσα στο διάστημα."
                        .formatted(trend > 0 ? "ανεβαίνει" : "πέφτει", Math.abs(trend)));
            }
        });
        if (!anomalies.isEmpty()) {
            Analysis.Anomaly worst = anomalies.get(0);
            findings.add("%d εποχικές ανωμαλίες· η εντονότερη: %s = %.1f, %s."
                    .formatted(anomalies.size(), worst.field(), worst.value(), worst.reason()));
        }

        String status = score >= 80 ? "OK" : score >= 55 ? "WATCH" : "ALERT";
        String headline = switch (status) {
            case "OK" -> "Ομαλές συνθήκες";
            case "WATCH" -> "Άστατες συνθήκες";
            default -> "Ακραίες συνθήκες";
        };
        return new Analysis.Verdict(score, status, headline, findings);
    }

    private static double share(List<Analysis.ClassCount> classes, String name) {
        return classes.stream().filter(c -> c.className().equals(name))
                .mapToDouble(Analysis.ClassCount::share).findFirst().orElse(0);
    }

    private static double[] column(Input input, Map<String, FieldRoles> roles, FieldRoles role) {
        return FieldRoles.first(roles, role).map(input::column).orElse(null);
    }


    private static double at(double[] column, int index) {
        return column == null ? Double.NaN : column[index];
    }

    private static double value(Map<String, Double> centroid, String field) {
        if (field == null) {
            return 0;
        }
        Double v = centroid.get(field);
        return v == null ? 0 : v;
    }
}
