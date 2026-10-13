package com.thesis.datavortex.analytics.web;

import com.thesis.datavortex.analytics.client.ApiClient;
import com.thesis.datavortex.analytics.client.ApiDtos;
import com.thesis.datavortex.analytics.profile.Analysis;
import com.thesis.datavortex.analytics.profile.DataProfile;
import com.thesis.datavortex.analytics.profile.ProfileRegistry;
import com.thesis.datavortex.analytics.service.AnalysisService;
import com.thesis.datavortex.analytics.service.TimeRange;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Το API που καταναλώνει το UI. Όλα περνούν από εδώ, ώστε ο browser να μιλά σε ένα μόνο
 * origin και το api-service να μη χρειάζεται να είναι εκτεθειμένο.
 */
@RestController
@RequestMapping("/api")
public class AnalyticsController {

    private final AnalysisService analysis;
    private final ApiClient api;
    private final ProfileRegistry profiles;

    public AnalyticsController(AnalysisService analysis, ApiClient api, ProfileRegistry profiles) {
        this.analysis = analysis;
        this.api = api;
        this.profiles = profiles;
    }

    @GetMapping("/sources")
    public List<ApiDtos.Source> sources() {
        return api.sources();
    }

    @GetMapping("/sources/{source}/fields")
    public List<ApiDtos.Field> fields(@PathVariable String source) {
        return api.fields(source);
    }

    /** Τα διαθέσιμα προφίλ ανάλυσης και τι κάνει το καθένα διαφορετικά. */
    @GetMapping("/profiles")
    public List<Map<String, String>> profiles() {
        return profiles.all().stream()
                .map(p -> Map.of("id", p.id(), "title", p.title(), "approach", p.approach()))
                .toList();
    }

    @GetMapping("/analysis")
    public Analysis analysis(@RequestParam String source,
                             @RequestParam(required = false, defaultValue = "6h") String range,
                             @RequestParam(required = false) Instant from,
                             @RequestParam(required = false) Instant to,
                             @RequestParam(required = false, defaultValue = "auto") String bucket,
                             @RequestParam(required = false, defaultValue = "auto") String profile) {
        TimeRange r = TimeRange.of(range, from, to);
        return analysis.analyse(source, r.from(), r.to(), bucket, profile);
    }

    /** Ο πίνακας κουβάδες × πεδία, για τα διαγράμματα: ένα request για όλα τα πεδία. */
    @GetMapping("/matrix")
    public ApiDtos.Matrix matrix(@RequestParam String source,
                                 @RequestParam(required = false, defaultValue = "6h") String range,
                                 @RequestParam(required = false) Instant from,
                                 @RequestParam(required = false) Instant to,
                                 @RequestParam(required = false, defaultValue = "auto") String bucket,
                                 @RequestParam(required = false, defaultValue = "avg") String agg) {
        TimeRange r = TimeRange.of(range, from, to);
        return api.matrix(source, r.from(), r.to(), bucket, agg);
    }

    @GetMapping("/series")
    public Map<String, Object> series(@RequestParam String source,
                                      @RequestParam String field,
                                      @RequestParam(required = false, defaultValue = "6h") String range,
                                      @RequestParam(required = false) Instant from,
                                      @RequestParam(required = false) Instant to,
                                      @RequestParam(required = false, defaultValue = "auto") String bucket) {
        TimeRange r = TimeRange.of(range, from, to);
        return api.series(source, field, r.from(), r.to(), bucket);
    }

    @GetMapping("/rates")
    public Map<String, Object> rates(@RequestParam(required = false) String source,
                                     @RequestParam(required = false, defaultValue = "6h") String range,
                                     @RequestParam(required = false) Instant from,
                                     @RequestParam(required = false) Instant to,
                                     @RequestParam(required = false, defaultValue = "auto") String bucket) {
        TimeRange r = TimeRange.of(range, from, to);
        return api.rates(source, r.from(), r.to(), bucket);
    }

    @GetMapping("/records")
    public Map<String, Object> records(@RequestParam(required = false) String source,
                                       @RequestParam(required = false, defaultValue = "6h") String range,
                                       @RequestParam(required = false) Instant from,
                                       @RequestParam(required = false) Instant to,
                                       @RequestParam(required = false) String cursor,
                                       @RequestParam(required = false, defaultValue = "50") int limit) {
        TimeRange r = TimeRange.of(range, from, to);
        return api.records(source, r.from(), r.to(), cursor, Math.min(Math.max(limit, 1), 500));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", String.valueOf(e.getMessage())));
    }

    /** Το api-service είναι κάτω ή αργεί: το λέμε καθαρά, αντί για γενικό 500. */
    @ExceptionHandler(RestClientException.class)
    public ResponseEntity<Map<String, String>> apiDown(RestClientException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "Το api-service δεν απαντά: " + e.getMessage()));
    }
}
