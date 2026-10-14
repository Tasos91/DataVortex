package com.thesis.kafkacollector.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.csv.CsvInspector;
import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.service.CsvIngestService;
import com.thesis.kafkacollector.service.IngestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

/**
 * Ο σωλήνας του CSV σε δύο βήματα, όπως τον δουλεύει ο χρήστης:
 * <ol>
 *   <li><b>{@code POST /api/v1/csv/inspect}</b> — «τι έχει μέσα;» Στέλνει το αρχείο (ή μόνο τις
 *       πρώτες γραμμές του) και παίρνει τις στήλες, τους τύπους, δείγματα τιμών και ένα
 *       <b>έτοιμο plan</b>.</li>
 *   <li><b>{@code POST /api/v1/csv/ingest}</b> — «πάρε αυτές τις στήλες». Στέλνει το αρχείο μαζί με
 *       το plan που πείραξε, και τα δεδομένα ξεκινούν τη διαδρομή Kafka → βάση → ανάλυση.</li>
 * </ol>
 * Η επιθεώρηση είναι ξεχωριστό βήμα επίτηδες: χωρίς αυτήν, το «γράψε τα fieldMappings» απαιτεί να
 * ξέρεις από πριν πώς λέγονται οι στήλες, τι τύπο έχουν και ποια είναι ο χρόνος.
 */
@RestController
@RequestMapping("/api/v1/csv")
public class CsvController {

    private static final Logger log = LoggerFactory.getLogger(CsvController.class);

    /** Πόσο διαβάζουμε για την επιθεώρηση: οι πρώτες γραμμές αρκούν, και ένα αρχείο 2 GB δεν μπαίνει στη μνήμη. */
    private static final int INSPECT_BYTES = 256 * 1024;

    private final CsvIngestService csvService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** Το ίδιο όριο ταυτόχρονων εισαγωγών με το {@link IngestionController}: ένας producer, ένα buffer. */
    private final Semaphore slots;

    public CsvController(CsvIngestService csvService,
                         @Value("${datavortex.max-concurrent-ingests:4}") int maxConcurrent) {
        this.csvService = csvService;
        this.slots = new Semaphore(maxConcurrent);
    }

    /**
     * Επιθεώρηση: δέχεται είτε multipart ({@code file}), είτε το σκέτο κείμενο στο σώμα, είτε
     * {@code ?path=} για αρχείο που είναι ήδη στον server.
     */
    @PostMapping(value = "/inspect", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> inspectUpload(@RequestPart("file") MultipartFile file) {
        try (InputStream in = file.getInputStream()) {
            return ResponseEntity.ok(CsvInspector.inspect(head(in), file.getOriginalFilename()));
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(error("Το αρχείο δεν διαβάστηκε: " + e.getMessage()));
        }
    }

    @PostMapping(value = "/inspect")
    public ResponseEntity<?> inspectBody(InputStream body,
                                         @RequestParam(required = false) String path,
                                         @RequestParam(required = false) String name) {
        try {
            if (path != null && !path.isBlank()) {
                try (InputStream in = Files.newInputStream(Path.of(path))) {
                    return ResponseEntity.ok(CsvInspector.inspect(head(in), name != null ? name : path));
                }
            }
            return ResponseEntity.ok(CsvInspector.inspect(head(body), name));
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(error("Το αρχείο δεν διαβάστηκε: " + e.getMessage()));
        }
    }

    /**
     * Εισαγωγή με multipart: {@code file} το CSV, {@code plan} το JSON με τις επιλογές.
     * <p>
     * Το αρχείο διαβάζεται σε <b>ροή</b>: στη μνήμη βρίσκεται μία γραμμή τη φορά, άρα το μέγεθος
     * δεν έχει όριο — σε αντίθεση με το {@code /upload}, όπου ολόκληρο το σώμα δένεται από το Jackson.
     */
    @PostMapping(value = "/ingest", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> ingestUpload(@RequestPart("file") MultipartFile file,
                                          @RequestPart("plan") String planJson) {
        CsvPlan plan;
        try {
            plan = objectMapper.readValue(planJson, CsvPlan.class);
        } catch (Exception e) {
            return bad(new IllegalArgumentException("Το plan δεν είναι έγκυρο JSON: " + e.getMessage()));
        }
        if (!slots.tryAcquire()) {
            return busy();
        }
        try (Reader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8), 1 << 16)) {
            return ok(csvService.ingest(reader, plan), file.getOriginalFilename());
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error(e.getMessage()));
        } catch (Exception e) {
            log.error("Η εισαγωγή CSV απέτυχε", e);
            return ResponseEntity.internalServerError().body(error(String.valueOf(e.getMessage())));
        } finally {
            slots.release();
        }
    }

    /**
     * Εισαγωγή χωρίς multipart: το plan ως JSON στο σώμα και το αρχείο στον server
     * ({@code plan.filePath} δεν υπάρχει, άρα δίνεται ως {@code ?path=}). Χρήσιμο για scripts και
     * για αρχεία που είναι ήδη στο μηχάνημα — δεν ανεβαίνουν δύο φορές.
     */
    @PostMapping("/ingest")
    public ResponseEntity<?> ingestPath(@RequestBody CsvPlan plan, @RequestParam String path) {
        if (!slots.tryAcquire()) {
            return busy();
        }
        try (Reader reader = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
            return ok(csvService.ingest(reader, plan), path);
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (java.nio.file.NoSuchFileException e) {
            return bad(new IllegalArgumentException("Δεν υπάρχει αρχείο: " + path));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error(e.getMessage()));
        } catch (Exception e) {
            log.error("Η εισαγωγή CSV απέτυχε", e);
            return ResponseEntity.internalServerError().body(error(String.valueOf(e.getMessage())));
        } finally {
            slots.release();
        }
    }

    /**
     * Τα πρώτα {@value #INSPECT_BYTES} bytes, κομμένα στην <b>τελευταία ολόκληρη γραμμή</b>: μια
     * μισή τελευταία γραμμή θα έδινε λάθος τύπο σε μια στήλη ή ακόμα και λάθος διαχωριστικό.
     */
    private static String head(InputStream in) throws IOException {
        byte[] buf = in.readNBytes(INSPECT_BYTES);
        String text = new String(buf, StandardCharsets.UTF_8);
        if (buf.length == INSPECT_BYTES) {
            int lastNewline = text.lastIndexOf('\n');
            if (lastNewline > 0) {
                text = text.substring(0, lastNewline + 1);
            }
        }
        return text;
    }

    private static ResponseEntity<?> ok(IngestResult r, String what) {
        var body = new java.util.LinkedHashMap<String, Object>();
        body.put("file", what);
        body.put("read", r.read());
        body.put("accepted", r.accepted());
        body.put("skipped", r.skipped());
        body.put("failed", r.failed());
        body.put("message", "Στο Kafka: " + r.accepted() + " από " + r.read() + " εγγραφές."
                + (r.skipped() > 0 ? " Άκυρες γραμμές: " + r.skipped() + "." : "")
                + (r.failed() > 0 ? " Απέτυχαν: " + r.failed() + "." : ""));
        return r.failed() > 0 ? ResponseEntity.status(HttpStatus.MULTI_STATUS).body(body) : ResponseEntity.ok(body);
    }

    private static ResponseEntity<?> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(error(e.getMessage()));
    }

    private static ResponseEntity<?> busy() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(error("Τρέχουν ήδη όσες εισαγωγές αντέχει ο producer. Ξαναδοκίμασε σε λίγο."));
    }

    private static java.util.Map<String, Object> error(String message) {
        return java.util.Map.of("error", message == null ? "άγνωστο σφάλμα" : message);
    }
}
