package com.thesis.datavortex.analytics.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Το analytics-ui είναι υγιές μόνο αν το api-service απαντά.
 * <p>
 * Χωρίς αυτό, το {@code /actuator/health} έλεγε UP ακόμα κι όταν η μοναδική πηγή δεδομένων ήταν
 * κάτω: ο έλεγχος υγείας απαντούσε για το ίδιο το process, όχι για το αν μπορεί να κάνει τη
 * δουλειά του. Με αυτό, ένα orchestrator ή ένα alert βλέπει το πρόβλημα χωρίς να το ψάξει.
 */
@Component("apiService")
public class ApiHealth implements HealthIndicator {

    private final ApiClient api;
    private final String baseUrl;

    public ApiHealth(ApiClient api, @Value("${datavortex.api-base-url:http://localhost:8082}") String baseUrl) {
        this.api = api;
        this.baseUrl = baseUrl;
    }

    @Override
    public Health health() {
        long started = System.nanoTime();
        try {
            int sources = api.sources().size();
            return Health.up()
                    .withDetail("url", baseUrl)
                    .withDetail("sources", sources)
                    .withDetail("latencyMs", (System.nanoTime() - started) / 1_000_000)
                    .build();
        } catch (RuntimeException e) {
            return Health.down()
                    .withDetail("url", baseUrl)
                    .withDetail("error", String.valueOf(e.getMessage()))
                    .build();
        }
    }
}
