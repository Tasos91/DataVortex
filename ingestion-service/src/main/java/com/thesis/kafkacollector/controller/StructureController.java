package com.thesis.kafkacollector.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.csv.CsvInspector;
import com.thesis.kafkacollector.format.IngestSpec;
import com.thesis.kafkacollector.format.StructureInspector;
import com.thesis.kafkacollector.service.GenericIngestService;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Ο ίδιος σωλήνας για <b>κάθε</b> δομή: CSV, JSON, NDJSON, XML.
 * <ol>
 *   <li><b>{@code POST /api/v1/inspect}</b> — «τι έχει μέσα;» Βρίσκει μορφή, πού είναι οι εγγραφές,
 *       ποια πεδία υπάρχουν και τι τύπο έχουν, και επιστρέφει ένα <b>έτοιμο σχέδιο</b>.</li>
 *   <li><b>{@code POST /api/v1/ingest}</b> — «πάρε αυτά τα πεδία». Το σχέδιο όπως το πείραξε ο
 *       χρήστης, μαζί με το αρχείο.</li>
 * </ol>
 * Τα παλιά {@code /api/v1/csv/*} παραμένουν και δείχνουν στην ίδια υλοποίηση.
 */
@RestController
@RequestMapping("/api/v1")
public class StructureController {

    private static final Logger log = LoggerFactory.getLogger(StructureController.class);
    /** Όσα διαβάζουμε για την επιθεώρηση: ένα αρχείο 2 GB πρέπει να κοστίζει όσο κι ένα 2 KB. */
    private static final int INSPECT_BYTES = 512 * 1024;

    private final GenericIngestService ingestService;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Semaphore slots;

    public StructureController(GenericIngestService ingestService,
                               @Value("${datavortex.max-concurrent-ingests:4}") int maxConcurrent) {
        this.ingestService = ingestService;
        this.slots = new Semaphore(maxConcurrent);
    }

    // ------------------------------------------------------------------ επιθεώρηση

    @PostMapping(value = "/inspect", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> inspectUpload(@RequestPart("file") MultipartFile file,
                                           @RequestParam(required = false) String format) {
        try (InputStream in = file.getInputStream()) {
            return ResponseEntity.ok(inspect(head(in), file.getOriginalFilename(), format));
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (Exception e) {
            return failed(e);
        }
    }

    @PostMapping("/inspect")
    public ResponseEntity<?> inspectBody(InputStream body,
                                         @RequestParam(required = false) String path,
                                         @RequestParam(required = false) String name,
                                         @RequestParam(required = false) String format) {
        try {
            if (path != null && !path.isBlank()) {
                try (InputStream in = Files.newInputStream(Path.of(path))) {
                    return ResponseEntity.ok(inspect(head(in), name != null ? name : path, format));
                }
            }
            return ResponseEntity.ok(inspect(head(body), name, format));
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (java.nio.file.NoSuchFileException e) {
            return bad(new IllegalArgumentException("Δεν υπάρχει αρχείο: " + path));
        } catch (Exception e) {
            return failed(e);
        }
    }

    /**
     * Η μορφή κρίνεται από το περιεχόμενο όταν δεν δηλώνεται. Το CSV έχει δικό του επιθεωρητή
     * (στήλες, διαχωριστικό, δεκαδικό σύμβολο)· οι δομημένες μορφές τον κοινό.
     */
    private Object inspect(String sample, String fileName, String declaredFormat) throws Exception {
        if (sample == null || sample.isBlank()) {
            throw new IllegalArgumentException("Το αρχείο είναι κενό.");
        }
        IngestSpec.Format format = IngestSpec.Format.parse(declaredFormat);
        if (format == null) {
            format = IngestSpec.Format.detect(fileName, sample);
        }
        if (format == IngestSpec.Format.CSV) {
            return csvProfile(CsvInspector.inspect(sample, fileName));
        }
        return StructureInspector.inspect(sample, format, fileName, mapper);
    }

    /**
     * Το προφίλ του CSV στο ίδιο σχήμα με των υπόλοιπων μορφών, ώστε το UI να έχει έναν δρόμο.
     * Οι στήλες γίνονται διαδρομές πρώτου επιπέδου — που ακριβώς είναι.
     */
    private Map<String, Object> csvProfile(CsvInspector.Profile csv) {
        java.util.List<Map<String, Object>> fields = new java.util.ArrayList<>(csv.columns().size());
        for (CsvInspector.Column c : csv.columns()) {
            Map<String, Object> field = new LinkedHashMap<>();
            field.put("path", c.name());
            field.put("type", c.type());
            field.put("present", c.present());
            field.put("missing", c.missing());
            field.put("distinct", c.distinct());
            field.put("min", c.min());
            field.put("max", c.max());
            field.put("samples", c.samples());
            field.put("role", c.role());
            fields.add(field);
        }
        com.thesis.kafkacollector.csv.CsvPlan plan = csv.suggestion();
        java.util.List<com.thesis.kafkacollector.format.FieldSpec> specs = new java.util.ArrayList<>();
        for (com.thesis.kafkacollector.csv.CsvPlan.Column c : plan.columns()) {
            specs.add(com.thesis.kafkacollector.format.FieldSpec.of(c.target(), c.name(), c.type()));
        }
        IngestSpec suggestion = new IngestSpec(plan.sourceId(), plan.topic(), plan.createTopicIfMissing(),
                plan.partitions(), "csv", null, specs, plan.timeColumn(), plan.timeFormat(),
                plan.keyColumn(), false,
                new IngestSpec.Options(plan.delimiter(), plan.decimal(), plan.hasHeader(), null));

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("format", "csv");
        out.put("recordPath", null);
        out.put("recordsSampled", csv.rowsSampled());
        out.put("fields", fields);
        out.put("suggestion", suggestion);
        out.put("notes", csv.notes());
        return out;
    }

    // ------------------------------------------------------------------ εισαγωγή

    @PostMapping(value = "/ingest", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> ingestUpload(@RequestPart("file") MultipartFile file,
                                          @RequestPart("spec") String specJson) {
        IngestSpec spec;
        try {
            spec = mapper.readValue(specJson, IngestSpec.class);
        } catch (Exception e) {
            return bad(new IllegalArgumentException("Το spec δεν είναι έγκυρο JSON: " + e.getMessage()));
        }
        if (!slots.tryAcquire()) {
            return busy();
        }
        try {
            IngestSpec.Format format = resolveFormat(spec, file);
            try (Reader reader = reader(file.getInputStream(), spec)) {
                return ok(ingestService.ingest(reader, spec, format), file.getOriginalFilename(), format);
            }
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error(e.getMessage()));
        } catch (Exception e) {
            log.error("Η εισαγωγή απέτυχε", e);
            return failed(e);
        } finally {
            slots.release();
        }
    }

    /** Για αρχεία που βρίσκονται ήδη στον διακομιστή: το σχέδιο στο σώμα, η διαδρομή ως παράμετρος. */
    @PostMapping("/ingest")
    public ResponseEntity<?> ingestPath(@RequestBody IngestSpec spec, @RequestParam String path) {
        if (!slots.tryAcquire()) {
            return busy();
        }
        try {
            Path file = Path.of(path);
            IngestSpec.Format format = spec.formatOrDetect(path, headOf(file));
            try (Reader reader = new BufferedReader(
                    new InputStreamReader(Files.newInputStream(file), spec.opts().charsetOrUtf8()), 1 << 16)) {
                return ok(ingestService.ingest(reader, spec, format), path, format);
            }
        } catch (IllegalArgumentException e) {
            return bad(e);
        } catch (java.nio.file.NoSuchFileException e) {
            return bad(new IllegalArgumentException("Δεν υπάρχει αρχείο: " + path));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error(e.getMessage()));
        } catch (Exception e) {
            log.error("Η εισαγωγή απέτυχε", e);
            return failed(e);
        } finally {
            slots.release();
        }
    }

    // ------------------------------------------------------------------ βοηθητικά

    private IngestSpec.Format resolveFormat(IngestSpec spec, MultipartFile file) throws IOException {
        IngestSpec.Format declared = IngestSpec.Format.parse(spec.format());
        if (declared != null) {
            return declared;
        }
        // Χωρίς δήλωση, κοιτάμε την αρχή του αρχείου. Ο InputStream του multipart ξαναδιαβάζεται
        // από την αρχή στο ingest, γιατί εδώ ανοίγουμε δικό μας.
        try (InputStream in = file.getInputStream()) {
            byte[] head = in.readNBytes(8192);
            return IngestSpec.Format.detect(file.getOriginalFilename(),
                    new String(head, spec.opts().charsetOrUtf8()));
        }
    }

    private static Reader reader(InputStream in, IngestSpec spec) {
        return new BufferedReader(new InputStreamReader(in, spec.opts().charsetOrUtf8()), 1 << 16);
    }

    private static String headOf(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            return new String(in.readNBytes(8192), StandardCharsets.UTF_8);
        }
    }

    /**
     * Τα πρώτα {@value #INSPECT_BYTES} bytes, κομμένα στην <b>τελευταία ολόκληρη γραμμή</b>: μια
     * μισή γραμμή θα έδινε λάθος τύπο σε πεδίο ή ακόμα και λάθος μορφή.
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

    private static ResponseEntity<?> ok(IngestResult r, String what, IngestSpec.Format format) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("file", what);
        body.put("format", format.name().toLowerCase(java.util.Locale.ROOT));
        body.put("read", r.read());
        body.put("accepted", r.accepted());
        body.put("skipped", r.skipped());
        body.put("failed", r.failed());
        body.put("message", "Στο Kafka: " + r.accepted() + " από " + r.read() + " εγγραφές."
                + (r.skipped() > 0 ? " Άκυρες: " + r.skipped() + "." : "")
                + (r.failed() > 0 ? " Απέτυχαν: " + r.failed() + "." : ""));
        return r.failed() > 0 ? ResponseEntity.status(HttpStatus.MULTI_STATUS).body(body) : ResponseEntity.ok(body);
    }

    private static ResponseEntity<?> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(error(e.getMessage()));
    }

    private static ResponseEntity<?> failed(Exception e) {
        return ResponseEntity.internalServerError().body(error(e.getClass().getSimpleName() + ": " + e.getMessage()));
    }

    private static ResponseEntity<?> busy() {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(error("Τρέχουν ήδη όσες εισαγωγές αντέχει ο producer. Ξαναδοκίμασε σε λίγο."));
    }

    private static Map<String, Object> error(String message) {
        return Map.of("error", message == null ? "άγνωστο σφάλμα" : message);
    }
}
