package com.thesis.kafkacollector.controller;

import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.service.FileStreamingService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class IngestionController {

    private final FileStreamingService streamingService;

    public IngestionController(FileStreamingService streamingService) {
        this.streamingService = streamingService;
    }

    @PostMapping("/upload")
    public ResponseEntity<String> startIngestion(@RequestBody IngestionRequest request) {
        try {
            if (request.getPayload() != null) {
                int sent = streamingService.processPayload(request);
                return ResponseEntity.ok("Στάλθηκαν " + sent + " μηνύματα στο Kafka με δυναμικά πεδία Protobuf.");
            } else if ("CSV".equalsIgnoreCase(request.getFileType())) {
                streamingService.processCsvStream(request);
            } else if ("JSON".equalsIgnoreCase(request.getFileType())) {
                streamingService.processJsonStream(request);
            } else {
                return ResponseEntity.badRequest().body("Δώσε είτε payload είτε fileType (CSV ή JSON) με filePath.");
            }
            return ResponseEntity.ok("Η εισαγωγή στον πίνακα ingested_records ολοκληρώθηκε!");
        } catch (IllegalArgumentException e) {
            // Λάθος στο request, π.χ. topic που δεν υπάρχει
            return ResponseEntity.badRequest().body("Σφάλμα: " + e.getMessage());
        } catch (IllegalStateException e) {
            // Η υποδομή δεν απαντά (π.χ. ο broker είναι κάτω)
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Σφάλμα: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Σφάλμα: " + e.getMessage());
        }
    }
}