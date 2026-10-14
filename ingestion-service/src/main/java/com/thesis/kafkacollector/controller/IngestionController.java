package com.thesis.kafkacollector.controller;

import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.service.FileStreamingService;
import com.thesis.kafkacollector.service.IngestResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.InputStream;
import java.util.concurrent.Semaphore;

@RestController
@RequestMapping("/api/v1")
public class IngestionController {

    private final FileStreamingService streamingService;
    /**
     * Όριο ταυτόχρονων εισαγωγών. Κάθε εισαγωγή γεμίζει το buffer του producer· χωρίς όριο,
     * αρκετά ταυτόχρονα requests μπλοκάρουν όλα τα Tomcat threads στο send(). Τα επιπλέον
     * παίρνουν 429 και ξαναπροσπαθούν, που είναι σωστό backpressure αντί για κόλλημα.
     */
    private final Semaphore slots;

    public IngestionController(FileStreamingService streamingService,
                               @Value("${datavortex.max-concurrent-ingests:4}") int maxConcurrent) {
        this.streamingService = streamingService;
        this.slots = new Semaphore(maxConcurrent);
    }

    @PostMapping("/upload")
    public ResponseEntity<String> startIngestion(@RequestBody IngestionRequest request) {
        if (!slots.tryAcquire()) {
            return tooManyRequests();
        }
        try {
            if (request.getPayload() != null) {
                return ok(streamingService.processPayload(request));
            } else if ("CSV".equalsIgnoreCase(request.getFileType())) {
                return ok(streamingService.processCsvStream(request));
            } else if ("JSON".equalsIgnoreCase(request.getFileType())) {
                return ok(streamingService.processJsonStream(request));
            } else {
                return ResponseEntity.badRequest().body("Δώσε είτε payload είτε fileType (CSV ή JSON) με filePath.");
            }
        } catch (IllegalArgumentException e) {
            // Λάθος στο request, π.χ. topic που δεν υπάρχει
            return ResponseEntity.badRequest().body("Σφάλμα: " + e.getMessage());
        } catch (IllegalStateException e) {
            // Η υποδομή δεν απαντά (π.χ. ο broker είναι κάτω)
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Σφάλμα: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Σφάλμα: " + e.getMessage());
        } finally {
            slots.release();
        }
    }

    /**
     * Εισαγωγή χωρίς όριο μεγέθους: NDJSON, ένα JSON αντικείμενο ανά γραμμή.
     * Η πρώτη γραμμή είναι οι ρυθμίσεις, οι επόμενες οι εγγραφές:
     *
     * <pre>
     * {"sourceId":"ais","topic":"ingest.ais","keyField":"$.mmsi","fieldMappings":{"Speed":"$.speed"}}
     * {"mmsi":1,"speed":12.5}
     * {"mmsi":2,"speed":9.0}
     * </pre>
     *
     * Σε αντίθεση με το /upload, εδώ στη μνήμη βρίσκεται μία γραμμή τη φορά.
     */
    @PostMapping("/stream")
    public ResponseEntity<String> streamIngestion(InputStream body) {
        if (!slots.tryAcquire()) {
            return tooManyRequests();
        }
        try {
            return ok(streamingService.processNdjson(body));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body("Σφάλμα: " + e.getMessage());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Σφάλμα: " + e.getMessage());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Σφάλμα: " + e.getMessage());
        } finally {
            slots.release();
        }
    }

    /**
     * Η απάντηση φεύγει αφού το Kafka επιβεβαιώσει τα μηνύματα, ώστε το 200 να σημαίνει
     * πραγματικά "τα δεδομένα είναι στο Kafka". Αν κάποια απέτυχαν, ο client το μαθαίνει.
     */
    private static ResponseEntity<String> ok(IngestResult result) {
        String body = "Στο Kafka: " + result.accepted() + " από " + result.read() + " εγγραφές"
                + (result.skipped() > 0 ? ", άκυρες: " + result.skipped() : "")
                + (result.failed() > 0 ? ", απέτυχαν: " + result.failed() : "")
                + ".";
        return result.failed() > 0
                ? ResponseEntity.status(HttpStatus.MULTI_STATUS).body(body)
                : ResponseEntity.ok(body);
    }

    private static ResponseEntity<String> tooManyRequests() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body("Τρέχουν ήδη όσες εισαγωγές αντέχει ο producer. Ξαναδοκίμασε σε λίγο.");
    }
}
