package com.thesis.datavortex.analytics.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Ο ΜΟΝΟΣ δρόμος αυτού του service προς τα δεδομένα: HTTP προς το api-service.
 * <p>
 * Δεν υπάρχει driver βάσης στο classpath, άρα η απομόνωση δεν είναι σύμβαση αλλά αδυναμία:
 * ακόμα κι αν κάποιος το ήθελε, δεν μπορεί να ανοίξει σύνδεση στη βάση από εδώ.
 */
@Component
public class ApiClient {

    private final RestClient http;

    public ApiClient(@Value("${datavortex.api-base-url:http://localhost:8082}") String baseUrl,
                     @Value("${datavortex.api-timeout-seconds:10}") long timeoutSeconds) {
        // Ρητά timeouts: ένα κολλημένο api-service δεν πρέπει να κρεμάσει το UI.
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(
                java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build());
        factory.setReadTimeout(Duration.ofSeconds(timeoutSeconds));
        // Το RestClient.Builder δεν είναι auto-configured με το starter-webmvc· το φτιάχνουμε εδώ.
        this.http = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    public List<ApiDtos.Source> sources() {
        return http.get().uri("/api/v1/sources")
                .retrieve().body(new ParameterizedTypeReference<>() {});
    }

    public List<ApiDtos.Field> fields(String source) {
        return http.get().uri(b -> b.path("/api/v1/sources/{s}/fields").build(source))
                .retrieve().body(new ParameterizedTypeReference<>() {});
    }

    /**
     * @param agg {@code avg} για τη συμπεριφορά, {@code max} για τις αιχμές. Ο μέσος όρος ενός
     *            κουβά εξαφανίζει τις αιχμές, άρα η ανίχνευση ανωμαλιών χρειάζεται και τα δύο.
     */
    public ApiDtos.Matrix matrix(String source, Instant from, Instant to, String bucket, String agg) {
        return http.get().uri(b -> range(b.path("/api/v1/matrix"), from, to, bucket)
                        .queryParam("source", source).queryParam("agg", agg).build())
                .retrieve().body(ApiDtos.Matrix.class);
    }

    /** Τα παρακάτω περνούν αυτούσια στο UI: το UI δεν μιλάει ποτέ απευθείας στο api-service. */
    public Map<String, Object> series(String source, String field, Instant from, Instant to, String bucket) {
        return http.get().uri(b -> range(b.path("/api/v1/series"), from, to, bucket)
                        .queryParam("source", source).queryParam("field", field).build())
                .retrieve().body(new ParameterizedTypeReference<>() {});
    }

    public Map<String, Object> rates(String source, Instant from, Instant to, String bucket) {
        return http.get().uri(b -> {
            UriBuilder builder = range(b.path("/api/v1/rates"), from, to, bucket);
            if (source != null && !source.isBlank()) {
                builder.queryParam("source", source);
            }
            return builder.build();
        }).retrieve().body(new ParameterizedTypeReference<>() {});
    }

    public Map<String, Object> records(String source, Instant from, Instant to, String cursor, int limit) {
        return http.get().uri(b -> {
            UriBuilder builder = b.path("/api/v1/records")
                    .queryParam("from", from.toString())
                    .queryParam("to", to.toString())
                    .queryParam("limit", limit);
            if (source != null && !source.isBlank()) {
                builder.queryParam("source", source);
            }
            if (cursor != null && !cursor.isBlank()) {
                builder.queryParam("cursor", cursor);
            }
            return builder.build();
        }).retrieve().body(new ParameterizedTypeReference<>() {});
    }

    private static UriBuilder range(UriBuilder builder, Instant from, Instant to, String bucket) {
        return builder.queryParam("from", from.toString())
                .queryParam("to", to.toString())
                .queryParam("bucket", bucket == null || bucket.isBlank() ? "auto" : bucket);
    }
}
