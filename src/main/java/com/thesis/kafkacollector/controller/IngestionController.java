package com.thesis.kafkacollector.controller;

import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.service.FileStreamingService;
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
            if ("CSV".equalsIgnoreCase(request.getFileType())) {
                streamingService.processCsvStream(request);
            } else if ("JSON".equalsIgnoreCase(request.getFileType())) {
                streamingService.processJsonStream(request);
            } else {
                return ResponseEntity.badRequest().body("Μη έγκυρος τύπος αρχείου. Υποστηρίζονται CSV και JSON.");
            }
            return ResponseEntity.ok("Η εισαγωγή στον πίνακα ingested_records ολοκληρώθηκε!");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Σφάλμα: " + e.getMessage());
        }
    }
}