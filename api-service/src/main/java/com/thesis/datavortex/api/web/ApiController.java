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
        range = trimPartial(range, b, to == null);
        return new Dtos.Series(source, field, b.id(), range[0], range[1],
                repo.series(source, field, range[0], range[1], b));
    }

    /** Ο πίνακας που τροφοδοτεί clustering και classification στο analytics-ui-service. */
    @GetMapping("/matrix")
    @Cacheable(value = "matrix",
            key = "#source + '|' + #fields + '|' + #from + '|' + #to + '|' + #bucket + '|' + #agg")
    public Dtos.Matrix matrix(@RequestParam String source,
                              @RequestParam(required = false) List<String> fields,
                              @RequestParam(required = false) Instant from,
                              @RequestParam(required = false) Instant to,
                              @RequestParam(required = false, defaultValue = "auto") String bucket,
                              @RequestParam(required = false, defaultValue = "avg") String agg) {
        Instant[] range = range(from, to);
        Buckets b = Buckets.of(bucket, range[0], range[1]);
        range = trimPartial(range, b, to == null);
        return repo.matrix(source, fields, range[0], range[1], b, agg);
    }

    @GetMapping("/rates")
    @Cacheable(value = "rates", key = "#source + '|' + #from + '|' + #to + '|' + #bucket")
    public Dtos.Rates rates(@RequestParam(required = false) String source,
                            @RequestParam(required = false) Instant from,
                            @RequestParam(required = false) Instant to,
                            @RequestParam(required = false, defaultValue = "auto") String bucket) {
        Instant[] range = range(from, to);
        Buckets b = Buckets.of(bucket, range[0], range[1]);
        range = trimPartial(range, b, to == null);
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

    /** Στρογγυλοποίηση στα 30s όταν λείπει το 'to': χωρίς αυτό κάθε request έχει άλλο κλειδί
     *  cache (το «τώρα» αλλάζει συνεχώς) και το cache δεν χτυπά ποτέ. */
    private static Instant snap(Instant moment) {
        long grain = 30_000L;
        return Instant.ofEpochMilli(moment.toEpochMilli() / grain * grain);
    }

    /**
     * Κόβει τον κουβά που ακόμα γεμίζει.
     * <p>
     * Ο τελευταίος κουβάς ενός ζωντανού παραθύρου περιέχει μόνο όσα πρόλαβαν να φτάσουν: ο
     * μέσος όρος του είναι μισοτελειωμένος και το πλήθος του μικρό, οπότε κάθε γράφημα
     * «βουτάει» στο τέλος και η ανάλυση βλέπει ανωμαλία εκεί που απλώς δεν πέρασε ακόμα η ώρα.
     * Το κριτήριο είναι ΑΝ το τέλος ακουμπά το τώρα, όχι αν το έστειλε ρητά ο client: το UI
     * στέλνει πάντα ρητά from/to (για να πετυχαίνει το cache), οπότε ένας έλεγχος «μου το
     * ζήτησαν;» δεν θα έπιανε ποτέ. Ιστορικά παράθυρα μένουν ανέπαφα.
     */
    private static Instant[] trimPartial(Instant[] range, Buckets bucket, boolean unusedEndIsNow) {
        long width = bucket.width().toMillis();
        if (range[1].isBefore(Instant.now().minusMillis(width))) {
            return range; // το παράθυρο έχει ήδη κλείσει, δεν γεμίζει τίποτα
        }
        Instant trimmed = Instant.ofEpochMilli(range[1].toEpochMilli() / width * width);
        return trimmed.isAfter(range[0]) ? new Instant[]{range[0], trimmed} : range;
    }

    private static Instant[] range(Instant from, Instant to) {
        Instant end = to != null ? to : snap(Instant.now());
        Instant start = from != null ? from : end.minus(DEFAULT_RANGE);
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("Το 'from' πρέπει να είναι πριν από το 'to'.");
        }
        return new Instant[]{start, end};
    }
}
