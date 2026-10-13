package com.thesis.datavortex.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Read-only REST πάνω από τη βάση. Είναι το ΜΟΝΟ service που διαβάζει από τη βάση:
 * ούτε το UI ούτε κανείς άλλος συνδέεται απευθείας.
 * <p>
 * Δεν τρέχει Flyway: το schema το κατέχει το ingestion-service. Εδώ μόνο διαβάζουμε.
 */
@SpringBootApplication
public class ApiServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiServiceApplication.class, args);
    }
}
