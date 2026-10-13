package com.thesis.datavortex.api.repo;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * Keyset cursor για τη σελιδοποίηση των ωμών εγγραφών.
 * <p>
 * Γιατί όχι {@code OFFSET}: το OFFSET 1.000.000 αναγκάζει τη βάση να διαβάσει και να πετάξει
 * ένα εκατομμύριο γραμμές, άρα η σελίδα 1000 είναι χιλιάδες φορές πιο αργή από την πρώτη.
 * Με keyset κάθε σελίδα κοστίζει το ίδιο: ένα index seek στο σημείο που σταματήσαμε.
 * <p>
 * Το κλειδί είναι το ίδιο με το PRIMARY KEY του πίνακα, άρα είναι πάντα μοναδικό και σταθερό.
 */
public record Cursor(Instant eventTime, String topic, int partition, long offset) {

    public String encode() {
        String raw = eventTime.toEpochMilli() + "|" + partition + "|" + offset + "|" + topic;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    public static Cursor decode(String encoded) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
            String[] parts = raw.split("\\|", 4);
            return new Cursor(Instant.ofEpochMilli(Long.parseLong(parts[0])),
                    parts[3], Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Άκυρο cursor.");
        }
    }
}
