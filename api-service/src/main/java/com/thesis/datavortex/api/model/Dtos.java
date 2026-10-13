package com.thesis.datavortex.api.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Ό,τι επιστρέφει το API. Μικρά records, χωρίς λογική. */
public final class Dtos {

    private Dtos() {}

    /** Μια πηγή δεδομένων, όπως τη βλέπει το UI στην αρχική λίστα. */
    public record Source(String sourceId, List<String> topics, long records,
                         Instant firstSeen, Instant lastSeen, int numericFields, int textFields) {}

    /**
     * Ένα πεδίο μιας πηγής.
     *
     * @param jsonType number / string / boolean / array / object / null — καθορίζει τι ανάλυση
     *                 μπορεί να γίνει πάνω του
     */
    public record Field(String field, String jsonType, long n, Instant firstSeen, Instant lastSeen) {}

    /** Ένα σημείο χρονοσειράς. Το {@code stddev} υπολογίζεται από sum και sum_sq. */
    public record Point(Instant t, long n, Double avg, Double min, Double max, Double stddev) {}

    public record Series(String source, String field, String bucket, Instant from, Instant to,
                         List<Point> points) {}

    /**
     * Πίνακας για clustering: γραμμές = χρονικοί κουβάδες, στήλες = πεδία.
     * Το ML τρέχει πάνω σε αυτό, δηλαδή σε εκατοντάδες κουβάδες και όχι σε εκατομμύρια γραμμές —
     * γι' αυτό μένει γρήγορο όσο κι αν μεγαλώσουν τα δεδομένα.
     */
    public record Matrix(String source, String bucket, Instant from, Instant to,
                         List<String> fields, List<Row> rows) {
        public record Row(Instant t, long n, Map<String, Double> values) {}
    }

    /** Σελίδα ωμών εγγραφών με keyset cursor (χωρίς OFFSET: δεν αργεί όσο προχωράς). */
    public record RecordPage(List<Entry> records, String nextCursor, boolean hasMore) {
        public record Entry(Instant eventTime, String sourceId, String topic, int partition,
                            long offset, Map<String, Object> fields) {}
    }

    public record RatePoint(Instant t, long records) {}

    public record Rates(String source, String bucket, Instant from, Instant to, List<RatePoint> points) {}
}
