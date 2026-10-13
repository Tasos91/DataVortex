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

    MINUTE("1m", "metric_1m", "records_1m", Duration.ofMinutes(1)),
    HOUR("1h", "metric_1h", null, Duration.ofHours(1)),
    DAY("1d", "metric_1d", null, Duration.ofDays(1));

    /** Πόσους κουβάδες θέλουμε το πολύ σε μία απάντηση (περίπου ένα pixel ο καθένας). */
    private static final int TARGET_BUCKETS = 600;

    private final String id;
    private final String metricView;
    private final String recordsView;
    private final Duration width;

    Buckets(String id, String metricView, String recordsView, Duration width) {
        this.id = id;
        this.metricView = metricView;
        this.recordsView = recordsView;
        this.width = width;
    }

    public String id() {
        return id;
    }

    public String metricView() {
        return metricView;
    }

    public Duration width() {
        return width;
    }

    /** Το records_1m υπάρχει μόνο σε λεπτό· τα χονδρότερα αθροίζονται από αυτό στο query. */
    public String recordsSourceView() {
        return MINUTE.recordsView;
    }

    public static Buckets of(String requested, Instant from, Instant to) {
        if (requested != null && !requested.isBlank() && !"auto".equalsIgnoreCase(requested)) {
            for (Buckets b : values()) {
                if (b.id.equalsIgnoreCase(requested)) {
                    return b;
                }
            }
            throw new IllegalArgumentException("Άγνωστο bucket '" + requested + "'. Δεκτά: auto, 1m, 1h, 1d.");
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
