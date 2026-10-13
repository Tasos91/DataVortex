package com.thesis.datavortex.analytics.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Το σχήμα των απαντήσεων του api-service, όπως το χρειάζεται αυτό το service.
 * <p>
 * Επίτηδες ξεχωριστά από τα DTO του api-service και όχι σε κοινό module: τα δύο services
 * επικοινωνούν μέσω HTTP και πρέπει να μπορούν να εξελίσσονται ανεξάρτητα. Το
 * {@code @JsonIgnoreProperties} σημαίνει ότι ένα νέο πεδίο στο API δεν σπάει το UI.
 */
public final class ApiDtos {

    private ApiDtos() {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Source(String sourceId, List<String> topics, long records,
                         Instant firstSeen, Instant lastSeen, int numericFields, int textFields) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Field(String field, String jsonType, long n, Instant firstSeen, Instant lastSeen) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Matrix(String source, String bucket, Instant from, Instant to,
                         List<String> fields, List<Row> rows) {

        @JsonIgnoreProperties(ignoreUnknown = true)
        public record Row(Instant t, long n, Map<String, Double> values) {}
    }
}
