package com.thesis.datavortex.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * UI και αναλύσεις (clustering, classification, ανίχνευση ανωμαλιών).
 * Παίρνει τα δεδομένα ΜΟΝΟ από το api-service μέσω HTTP· δεν έχει καμία σύνδεση με τη βάση.
 */
@SpringBootApplication
public class AnalyticsUiApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsUiApplication.class, args);
    }
}
