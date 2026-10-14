package com.thesis.datavortex.analytics.profile;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ελέγχει ότι κάθε προφίλ βγάζει το σωστό <b>νόημα</b>, όχι μόνο ότι δεν σκάει:
 * ότι η τηλεμετρία βρίσκει τα καθεστώτα και την υποβάθμιση, και ότι ο καιρός βρίσκει
 * τη ζεστή νύχτα που ένα καθολικό z-score θα έκρυβε.
 */
class AnalysisBehaviourTest {

    private static final Instant START = Instant.parse("2026-10-14T00:00:00Z");

    private static DataProfile.Input input(List<String> fields, List<Map<String, Double>> rows,
                                           Duration step) {
        return input(fields, rows, rows, step);
    }

    private static DataProfile.Input input(List<String> fields, List<Map<String, Double>> rows,
                                           List<Map<String, Double>> peaks, Duration step) {
        List<Instant> times = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            times.add(START.plus(step.multipliedBy(i)));
        }
        return new DataProfile.Input("test", "1m", times.get(0),
                times.get(times.size() - 1), fields, times, rows, peaks);
    }

    private static Map<String, Double> row(Object... pairs) {
        Map<String, Double> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], ((Number) pairs[i + 1]).doubleValue());
        }
        return map;
    }

    // ---------------------------------------------------------------- τηλεμετρία

    @Test
    void telemetryFindsIdleAndSaturatedRegimes() {
        List<String> fields = List.of("GpuUtil", "LatencyMs");
        List<Map<String, Double>> rows = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            rows.add(i < 30 ? row("GpuUtil", 8 + i % 3, "LatencyMs", 12 + i % 2)
                    : row("GpuUtil", 95 + i % 3, "LatencyMs", 190 + i % 5));
        }
        Analysis analysis = new AiTelemetryProfile().analyse(input(fields, rows, Duration.ofMinutes(1)));

        assertEquals("ai-telemetry", analysis.profile());
        assertEquals(2, analysis.clusters().size(), "δύο καθαρά καθεστώτα");
        List<String> labels = analysis.clusters().stream().map(Analysis.Cluster::label).toList();
        assertTrue(labels.contains("αδράνεια"), "το χαμηλό φορτίο πρέπει να λέγεται αδράνεια: " + labels);
        assertTrue(labels.contains("κορεσμός"), "το υψηλό φορτίο πρέπει να λέγεται κορεσμός: " + labels);

        // Το κέντρο πρέπει να είναι σε πραγματικές μονάδες, αλλιώς δεν λέει τίποτα σε άνθρωπο.
        Analysis.Cluster idle = analysis.clusters().stream()
                .filter(c -> c.label().equals("αδράνεια")).findFirst().orElseThrow();
        assertEquals(9, idle.centroid().get("GpuUtil"), 1.5);
    }

    @Test
    void telemetryFlagsSlowWithoutLoadAsDegradation() {
        // Το σημαντικό εύρημα: η καθυστέρηση εκτοξεύεται ενώ η χρήση μένει χαμηλή.
        List<String> fields = List.of("GpuUtil", "LatencyMs");
        List<Map<String, Double>> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            rows.add(row("GpuUtil", 10 + i % 3, "LatencyMs", 12 + i % 2));
        }
        rows.set(20, row("GpuUtil", 11, "LatencyMs", 900));
        rows.set(21, row("GpuUtil", 10, "LatencyMs", 870));

        Analysis analysis = new AiTelemetryProfile().analyse(input(fields, rows, Duration.ofMinutes(1)));

        assertTrue(analysis.classes().stream().anyMatch(c -> c.className().equals("υποβάθμιση")),
                "πρέπει να υπάρχει κατηγορία υποβάθμισης: " + analysis.classes());
        assertTrue(analysis.anomalies().stream()
                        .anyMatch(a -> a.reason().contains("χαμηλή χρήση")),
                "πρέπει να εξηγεί ότι είναι αργό χωρίς φορτίο: " + analysis.anomalies());
        assertTrue(analysis.verdict().findings().stream()
                        .anyMatch(f -> f.contains("ΧΩΡΙΣ")),
                "η κρίση πρέπει να το αναφέρει: " + analysis.verdict().findings());
    }

    @Test
    void telemetrySeesSpikesHiddenInsideBucketAverages() {
        // Μέσοι όροι ήρεμοι, αιχμές τεράστιες: χωρίς τις αιχμές δεν θα βρισκόταν τίποτα.
        List<String> fields = List.of("GpuUtil", "LatencyMs");
        List<Map<String, Double>> averages = new ArrayList<>();
        List<Map<String, Double>> peaks = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            averages.add(row("GpuUtil", 50 + i % 3, "LatencyMs", 40 + i % 2));
            peaks.add(row("GpuUtil", 55 + i % 3, "LatencyMs", 45 + i % 2));
        }
        peaks.set(25, row("GpuUtil", 56, "LatencyMs", 4000));

        Analysis analysis = new AiTelemetryProfile()
                .analyse(input(fields, averages, peaks, Duration.ofMinutes(1)));

        assertTrue(analysis.anomalies().stream().anyMatch(a -> a.value() > 3000),
                "η αιχμή των 4000ms πρέπει να βρεθεί: " + analysis.anomalies());
        assertTrue(analysis.anomalies().stream().anyMatch(a -> a.reason().contains("αιχμή")));
    }

    @Test
    void calmTelemetryGetsAHighScoreAndNoFalseAlarms() {
        List<String> fields = List.of("GpuUtil", "LatencyMs");
        List<Map<String, Double>> rows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            rows.add(row("GpuUtil", 45 + (i % 5), "LatencyMs", 30 + (i % 4)));
        }
        Analysis analysis = new AiTelemetryProfile().analyse(input(fields, rows, Duration.ofMinutes(1)));

        assertEquals("OK", analysis.verdict().status());
        assertTrue(analysis.verdict().score() >= 80, "ήρεμα δεδομένα: " + analysis.verdict().score());
        assertTrue(analysis.anomalies().isEmpty(), "χωρίς ψευδείς συναγερμούς: " + analysis.anomalies());
    }

    // ---------------------------------------------------------------- καιρός

    @Test
    void weatherCatchesTheHotNightThatGlobalStatsWouldHide() {
        // Τρεις μέρες ημερήσιου κύκλου ανά ώρα, και μία ζεστή νύχτα στη μέση.
        List<String> fields = List.of("TempC", "Humidity");
        List<Map<String, Double>> rows = new ArrayList<>();
        java.util.Random noise = new java.util.Random(4);
        for (int hour = 0; hour < 72; hour++) {
            // Λίγος θόρυβος, όπως κάθε πραγματικός αισθητήρας: χωρίς αυτόν η διασπορά είναι
            // ακριβώς μηδέν και κανένα στατιστικό μέτρο απόκλισης δεν ορίζεται.
            double temp = 20 + 8 * Math.sin((hour % 24 - 9) / 24.0 * 2 * Math.PI) + noise.nextGaussian() * 0.3;
            rows.add(row("TempC", temp, "Humidity", 60 + noise.nextGaussian()));
        }
        int hotNight = 26; // ώρα 02:00 της δεύτερης μέρας
        rows.set(hotNight, row("TempC", 30.0, "Humidity", 60.0));

        Analysis analysis = new WeatherProfile().analyse(input(fields, rows, Duration.ofHours(1)));

        assertEquals("weather", analysis.profile());
        // 30 βαθμοί είναι απόλυτα συνηθισμένοι για το μεσημέρι, άρα ένα καθολικό z-score δεν
        // θα έλεγε τίποτα. Η σύγκριση με την ΙΔΙΑ ώρα της ημέρας τα βρίσκει.
        assertTrue(analysis.anomalies().stream().anyMatch(a ->
                        a.field().equals("TempC") && a.value() == 30.0),
                "η ζεστή νύχτα πρέπει να εντοπιστεί: " + analysis.anomalies());
        assertTrue(analysis.anomalies().get(0).reason().contains("ώρα"),
                "η εξήγηση πρέπει να αναφέρει την ώρα: " + analysis.anomalies().get(0).reason());
    }

    @Test
    void weatherUsesRainIntensityNotASingleDrop() {
        // Μέση ένταση ~0: δεν είναι βροχερή ώρα, όσο μεγάλη κι αν είναι η στιγμιαία αιχμή.
        List<String> fields = List.of("TempC", "RainMm");
        List<Map<String, Double>> averages = new ArrayList<>();
        List<Map<String, Double>> peaks = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            averages.add(row("TempC", 18.0 + i % 3, "RainMm", 0.0));
            peaks.add(row("TempC", 19.0 + i % 3, "RainMm", 4.0));
        }
        Analysis analysis = new WeatherProfile()
                .analyse(input(fields, averages, peaks, Duration.ofHours(1)));

        assertTrue(analysis.classes().stream().noneMatch(c -> c.className().equals("βροχή")),
                "μία στάλα δεν κάνει βροχερή ώρα: " + analysis.classes());
    }

    @Test
    void weatherNamesRainyRegimeWhenItActuallyRains() {
        List<String> fields = List.of("TempC", "RainMm", "Humidity");
        List<Map<String, Double>> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            boolean storm = i >= 20;
            rows.add(row("TempC", storm ? 15.0 + i % 2 : 25.0 + i % 2,
                    "RainMm", storm ? 2.5 : 0.0,
                    "Humidity", storm ? 90.0 : 45.0));
        }
        Analysis analysis = new WeatherProfile().analyse(input(fields, rows, Duration.ofHours(1)));

        assertTrue(analysis.clusters().stream().anyMatch(c -> c.label().startsWith("βροχερό")),
                "πρέπει να υπάρχει βροχερό καθεστώς: "
                        + analysis.clusters().stream().map(Analysis.Cluster::label).toList());
        assertTrue(analysis.classes().stream().anyMatch(c -> c.className().equals("βροχή")));
    }

    @Test
    void weatherSeparatesASteadyDrizzleFromAPassingShower() {
        // Ίδιος μέσος όρος, εντελώς διαφορετικός καιρός: αριστερά βρέχει σταθερά όλο το εξάωρο,
        // δεξιά έπεσε μια μπόρα μισής ώρας. Με μόνο τον μέσο, και τα δύο έβγαιναν «βροχή» — έτσι
        // το 30% των ωριαίων μετρήσεων γινόταν 96% των κουβάδων και το UI ανακοίνωνε «βροχή 98%».
        List<String> fields = List.of("TempC", "RainMm");
        List<Map<String, Double>> steadyAvg = new ArrayList<>();
        List<Map<String, Double>> steadyPeak = new ArrayList<>();
        List<Map<String, Double>> burstAvg = new ArrayList<>();
        List<Map<String, Double>> burstPeak = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            steadyAvg.add(row("TempC", 18.0 + i % 3, "RainMm", 0.4));
            steadyPeak.add(row("TempC", 19.0 + i % 3, "RainMm", 0.5));   // λόγος 0,80 — σταθερό
            burstAvg.add(row("TempC", 18.0 + i % 3, "RainMm", 0.4));
            burstPeak.add(row("TempC", 19.0 + i % 3, "RainMm", 5.0));    // λόγος 0,08 — στιγμιαίο
        }
        Analysis steady = new WeatherProfile()
                .analyse(input(fields, steadyAvg, steadyPeak, Duration.ofHours(6)));
        Analysis burst = new WeatherProfile()
                .analyse(input(fields, burstAvg, burstPeak, Duration.ofHours(6)));

        assertTrue(steady.classes().stream().anyMatch(c -> c.className().equals("βροχή")),
                "σταθερή βροχή πρέπει να χαρακτηρίζει τον κουβά: " + steady.classes());
        assertTrue(burst.classes().stream().noneMatch(c -> c.className().equals("βροχή")),
                "μια μπόρα δεν κάνει βροχερό εξάωρο: " + burst.classes());
    }

    @Test
    void weatherDoesNotCallEveryRegimeRainyWhenItDrizzlesEverywhere() {
        // Ψιχάλα παντού, αλλά η μία ομάδα είναι ζεστή και η άλλη ψυχρή. Με απόλυτο κατώφλι και
        // μόνο, και οι δύο έβγαιναν «βροχερό 1» και «βροχερό 2»: αληθές, άχρηστο, και κρύβει
        // ακριβώς αυτό που τις ξεχωρίζει.
        List<String> fields = List.of("TempC", "RainMm", "Humidity");
        List<Map<String, Double>> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            boolean cold = i >= 20;
            rows.add(row("TempC", cold ? 8.0 + i % 2 : 28.0 + i % 2,
                    "RainMm", 0.4 + (i % 3) * 0.02,
                    "Humidity", cold ? 70.0 : 45.0));
        }
        Analysis analysis = new WeatherProfile().analyse(input(fields, rows, Duration.ofHours(1)));

        List<String> labels = analysis.clusters().stream().map(Analysis.Cluster::label).toList();
        assertTrue(labels.stream().noneMatch(l -> l.startsWith("βροχερό")),
                "καμία ομάδα δεν ξεχωρίζει στη βροχή, άρα το όνομα πρέπει να λέει κάτι άλλο: " + labels);
        assertEquals(labels.size(), labels.stream().distinct().count(),
                "τα ονόματα πρέπει να διαφέρουν μεταξύ τους: " + labels);
    }

    // ---------------------------------------------------------------- γενικό

    @Test
    void genericFindsDenseGroupsAndNamesTheOddOnesOut() {
        List<String> fields = List.of("AlphaValue", "BetaValue");
        List<Map<String, Double>> rows = new ArrayList<>();
        java.util.Random random = new java.util.Random(13);
        for (int i = 0; i < 30; i++) {
            rows.add(row("AlphaValue", 10 + random.nextGaussian() * 0.2,
                    "BetaValue", 100 + random.nextGaussian() * 0.2));
        }
        for (int i = 0; i < 30; i++) {
            rows.add(row("AlphaValue", 40 + random.nextGaussian() * 0.2,
                    "BetaValue", 20 + random.nextGaussian() * 0.2));
        }
        rows.add(row("AlphaValue", 500, "BetaValue", -500)); // ξεκάθαρο outlier

        Analysis analysis = new GenericProfile().analyse(input(fields, rows, Duration.ofMinutes(1)));

        assertEquals("generic", analysis.profile());
        assertTrue(analysis.algorithm().startsWith("DBSCAN"), "το γενικό προφίλ χρησιμοποιεί DBSCAN");
        assertTrue(analysis.classes().stream().anyMatch(c -> c.className().equals("απομακρυσμένο")),
                "το outlier πρέπει να ονομαστεί απομακρυσμένο: " + analysis.classes());
        assertEquals(2, analysis.clusters().size() - 1,
                "δύο πυκνές περιοχές συν την ομάδα των απομακρυσμένων: "
                        + analysis.clusters().stream().map(Analysis.Cluster::label).toList());
        assertTrue(analysis.verdict().findings().stream().anyMatch(f -> f.contains("περιγραφικά")),
                "πρέπει να λέει ότι τα πεδία δεν αναγνωρίστηκαν");
    }

    @Test
    void tooFewPointsSaysSoInsteadOfInventingClusters() {
        List<String> fields = List.of("A");
        List<Map<String, Double>> rows = List.of(row("A", 1), row("A", 2));
        for (DataProfile profile : List.of(new AiTelemetryProfile(), new WeatherProfile(), new GenericProfile())) {
            Analysis analysis = profile.analyse(input(fields, rows, Duration.ofMinutes(1)));
            assertEquals("NO_DATA", analysis.verdict().status(), profile.id());
            assertTrue(analysis.clusters().isEmpty());
        }
    }
}
