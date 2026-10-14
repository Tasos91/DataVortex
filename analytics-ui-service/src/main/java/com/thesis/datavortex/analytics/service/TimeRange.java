package com.thesis.datavortex.analytics.service;

import java.time.Duration;
import java.time.Instant;

/**
 * Διάστημα από σχετική έκφραση ({@code 6h}, {@code 7d}) ή ρητά from/to.
 * <p>
 * Το UI δουλεύει με κουμπιά «τελευταίες 6 ώρες», άρα δεν έχει νόημα να φτιάχνει ISO timestamps
 * και να τα ξαναστέλνει· επίσης ένα σταθερό `range` επιτρέπει στο cache να πετυχαίνει.
 */
public record TimeRange(Instant from, Instant to) {

    private static final Duration DEFAULT = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofDays(3650);
    /**
     * Σε πόσο "χοντρά" βήματα στρογγυλοποιείται το τέλος ενός σχετικού διαστήματος.
     * <p>
     * Χωρίς αυτό, το {@code range=6h} σημαίνει "μέχρι τώρα" και το «τώρα» αλλάζει σε κάθε
     * χιλιοστό: κάθε request παίρνει άλλο κλειδί cache, άρα το cache <b>δεν χτυπά ποτέ</b> και
     * η ακριβή ανάλυση ξανατρέχει ολόκληρη σε κάθε ανανέωση της σελίδας. Με στρογγυλοποίηση
     * στα 30 δευτερόλεπτα, όσοι ζητούν το ίδιο παράθυρο μοιράζονται το ίδιο αποτέλεσμα.
     */
    private static final Duration GRAIN = Duration.ofSeconds(30);

    public static TimeRange of(String range, Instant from, Instant to) {
        if (from != null && to != null) {
            if (!from.isBefore(to)) {
                throw new IllegalArgumentException("Το 'from' πρέπει να είναι πριν από το 'to'.");
            }
            return new TimeRange(from, to);
        }
        Instant end = to != null ? to : snap(Instant.now());
        Duration window = parse(range);
        Instant start = from != null ? from : end.minus(window);
        return new TimeRange(start, end);
    }

    /** Στρογγυλοποίηση προς τα κάτω στο {@link #GRAIN}. */
    private static Instant snap(Instant moment) {
        long grain = GRAIN.toMillis();
        return Instant.ofEpochMilli(moment.toEpochMilli() / grain * grain);
    }

    /** Δέχεται {@code 30m}, {@code 6h}, {@code 7d}, {@code 4w}. */
    public static Duration parse(String range) {
        if (range == null || range.isBlank()) {
            return DEFAULT;
        }
        String text = range.trim().toLowerCase();
        try {
            long amount = Long.parseLong(text.substring(0, text.length() - 1));
            Duration duration = switch (text.charAt(text.length() - 1)) {
                case 'm' -> Duration.ofMinutes(amount);
                case 'h' -> Duration.ofHours(amount);
                case 'd' -> Duration.ofDays(amount);
                case 'w' -> Duration.ofDays(amount * 7);
                default -> throw new IllegalArgumentException("μονάδα");
            };
            if (duration.isNegative() || duration.isZero() || duration.compareTo(MAX) > 0) {
                throw new IllegalArgumentException("εύρος");
            }
            return duration;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Άκυρο range '" + range + "'. Παραδείγματα: 30m, 6h, 24h, 7d, 4w.");
        }
    }

    public Duration duration() {
        return Duration.between(from, to);
    }
}
