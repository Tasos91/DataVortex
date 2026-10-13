package com.thesis.datavortex.api.web;

import com.thesis.datavortex.api.model.Dtos;
import com.thesis.datavortex.api.repo.AnalyticsRepository;
import com.thesis.datavortex.api.repo.Buckets;
import com.thesis.datavortex.api.repo.Cursor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Το read-only REST API πάνω από τη βάση. Το UI δεν γνωρίζει ούτε SQL ούτε schema.
 * <p>
 * Όλα τα συγκεντρωτικά endpoints διαβάζουν continuous aggregates, άρα ο χρόνος απόκρισης
 * εξαρτάται από το πλήθος των κουβάδων που ζητήθηκαν και όχι από τον όγκο των δεδομένων.
 */
@RestController
@RequestMapping("/api/v1")
public class ApiController {

    private static final Duration DEFAULT_RANGE = Duration.ofHours(6);

    private final AnalyticsRepository repo;

    public ApiController(AnalyticsRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/sources")
    @Cacheable("meta")
    public List<Dtos.Source> sources() {
        return repo.sources();
    }

    @GetMapping("/sources/{source}/fields")
    @Cacheable(value = "meta", key = "'fields:' + #source")
    public List<Dtos.Field> fields(@PathVariable String source) {
        return repo.fields(source);
    }

    @GetMapping("/series")
    @Cacheable(value = "series", key = "#source + '|' + #field + '|' + #from + '|' + #to + '|' + #bucket")
    public Dtos.Series series(@RequestParam String source,
                              @RequestParam String field,
                              @RequestParam(required = false) Instant from,
                              @RequestParam(required = false) Instant to,
                              @RequestParam(required = false, defaultValue = "auto") String bucket) {
        Instant[] range = range(from, to);
        Buckets b = Buckets.of(bucket, range[0], range[1]);
        return new Dtos.Series(source, field, b.id(), range[0], range[1],
                repo.series(source, field, range[0], range[1], b));
    }

    /** Ο πίνακας που τροφοδοτεί clustering και classification στο analytics-ui-service. */
    @GetMapping("/matrix")
    @Cacheable(value = "matrix", key = "#source + '|' + #fields + '|' + #from + '|' + #to + '|' + #bucket")
    public Dtos.Matrix matrix(@RequestParam String source,
                              @RequestParam(required = false) List<String> fields,
                              @RequestParam(required = false) Instant from,
                              @RequestParam(required = false) Instant to,
                              @RequestParam(required = false, defaultValue = "auto") String bucket) {
        Instant[] range = range(from, to);
        Buckets b = Buckets.of(bucket, range[0], range[1]);
        return repo.matrix(source, fields, range[0], range[1], b);
    }

    @GetMapping("/rates")
    @Cacheable(value = "rates", key = "#source + '|' + #from + '|' + #to + '|' + #bucket")
    public Dtos.Rates rates(@RequestParam(required = false) String source,
                            @RequestParam(required = false) Instant from,
                            @RequestParam(required = false) Instant to,
                            @RequestParam(required = false, defaultValue = "auto") String bucket) {
        Instant[] range = range(from, to);
        Buckets b = Buckets.of(bucket, range[0], range[1]);
        return new Dtos.Rates(source, b.id(), range[0], range[1], repo.rates(source, range[0], range[1], b));
    }

    /**
     * Ωμές εγγραφές με keyset cursor. Δεν υπάρχει {@code page=N}: η σελίδα 1000 κοστίζει όσο
     * και η πρώτη, ενώ με OFFSET θα κόστιζε χιλιάδες φορές περισσότερο.
     */
    @GetMapping("/records")
    public ResponseEntity<Dtos.RecordPage> records(@RequestParam(required = false) String source,
                                                   @RequestParam(required = false) String topic,
                                                   @RequestParam(required = false) Instant from,
                                                   @RequestParam(required = false) Instant to,
                                                   @RequestParam(required = false) String cursor,
                                                   @RequestParam(required = false, defaultValue = "100") int limit) {
        Instant[] range = range(from, to);
        Dtos.RecordPage page = repo.records(source, topic, range[0], range[1],
                cursor == null || cursor.isBlank() ? null : Cursor.decode(cursor), limit);
        // Οι ωμές εγγραφές δεν μπαίνουν στο cache (πολλοί διαφορετικοί cursors), αλλά
        // επιτρέπουμε σύντομο caching στον browser.
        return ResponseEntity.ok().cacheControl(CacheControl.maxAge(Duration.ofSeconds(5))).body(page);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }

    private static Instant[] range(Instant from, Instant to) {
        Instant end = to != null ? to : Instant.now();
        Instant start = from != null ? from : end.minus(DEFAULT_RANGE);
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("Το 'from' πρέπει να είναι πριν από το 'to'.");
        }
        return new Instant[]{start, end};
    }
}
