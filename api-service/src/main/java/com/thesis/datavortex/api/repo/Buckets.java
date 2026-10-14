package com.thesis.datavortex.api.repo;

import java.time.Duration;
import java.time.Instant;

/**
 * Επιλογή ανάλυσης (level of detail): ποιο συγκεντρωτικό θα διαβαστεί για το ζητούμενο διάστημα.
 * <p>
 * Αυτός είναι ο λόγος που το API μένει γρήγορο ανεξάρτητα από τον όγκο: ένα query επιστρέφει
 * πάντα λίγες εκατοντάδες κουβάδες, είτε ζητάς 10 λεπτά είτε 10 χρόνια.
 */
public enum Buckets {

    // Κλίμακα αναλύσεων με ΟΜΑΛΑ σκαλοπάτια. Με μόνο 1m/1h/1d, ένα παράθυρο 12 ωρών έπεφτε
    // από 720 κουβάδες σε 12: το διάγραμμα γινόταν άχρηστο και η ομαδοποίηση έβρισκε ομάδες
    // από 1-2 σημεία. Τα ενδιάμεσα επίπεδα προκύπτουν με επαναϋπολογισμό πάνω στο αμέσως
    // λεπτομερέστερο συγκεντρωτικό, που είναι ήδη μικρό — δεν χρειάζονται νέα views.
    MINUTE("1m", "metric_1m", Duration.ofMinutes(1)),
    FIVE_MINUTES("5m", "metric_1m", Duration.ofMinutes(5)),
    QUARTER_HOUR("15m", "metric_1m", Duration.ofMinutes(15)),
    HOUR("1h", "metric_1h", Duration.ofHours(1)),
    SIX_HOURS("6h", "metric_1h", Duration.ofHours(6)),
    DAY("1d", "metric_1d", Duration.ofDays(1));

    /** Πόσους κουβάδες θέλουμε το πολύ σε μία απάντηση (περίπου ένα pixel ο καθένας). */
    private static final int TARGET_BUCKETS = 800;

    private final String id;
    private final String metricView;
    private final Duration width;

    Buckets(String id, String metricView, Duration width) {
        this.id = id;
        this.metricView = metricView;
        this.width = width;
    }

    public String id() {
        return id;
    }

    /** Από ποιο συγκεντρωτικό διαβάζουμε. */
    public String metricView() {
        return metricView;
    }

    public Duration width() {
        return width;
    }

    public static Buckets of(String requested, Instant from, Instant to) {
        if (requested != null && !requested.isBlank() && !"auto".equalsIgnoreCase(requested)) {
            for (Buckets b : values()) {
                if (b.id.equalsIgnoreCase(requested)) {
                    return b;
                }
            }
            throw new IllegalArgumentException("Άγνωστο bucket '" + requested
                    + "'. Δεκτά: auto, 1m, 5m, 15m, 1h, 6h, 1d.");
        }
        long seconds = Duration.between(from, to).getSeconds();
        if (seconds <= 0) {
            return MINUTE;
        }
        for (Buckets b : values()) {
            if (seconds / b.width.getSeconds() <= TARGET_BUCKETS) {
                return b;
            }
        }
        return DAY;
    }
}
