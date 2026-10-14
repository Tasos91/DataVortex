package com.thesis.kafkacollector.controller;

import com.thesis.kafkacollector.service.AggregateRefresher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

/**
 * Χειροκίνητη ανανέωση των συγκεντρωτικών για ένα εύρος χρόνου.
 * <p>
 * Το {@link AggregateRefresher} το κάνει μόνο του για ό,τι γράφεται από εδώ και πέρα, αλλά
 * χρειάζεται και ρητός δρόμος: δεδομένα που μπήκαν πριν υπάρξει ο μηχανισμός, επαναφορά από
 * backup, ή χειροκίνητο {@code INSERT} δεν περνούν από τον writer και άρα δεν δηλώνονται πουθενά.
 */
@RestController
@RequestMapping("/api/v1/aggregates")
public class AggregatesController {

    private final AggregateRefresher refresher;

    public AggregatesController(AggregateRefresher refresher) {
        this.refresher = refresher;
    }

    /**
     * @param from ISO-8601, π.χ. {@code 2026-09-01T00:00:00Z}
     * @param to   ISO-8601· η ανανέωση καλύπτει ολόκληρες ημέρες γύρω από το εύρος
     */
    @PostMapping("/refresh")
    public ResponseEntity<Map<String, Object>> refresh(@RequestParam String from, @RequestParam String to) {
        long start;
        long end;
        try {
            start = Instant.parse(from).toEpochMilli();
            end = Instant.parse(to).toEpochMilli();
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(Map.of("error",
                    "Το from/to θέλει ISO-8601, π.χ. 2026-09-01T00:00:00Z: " + e.getMessage()));
        }
        if (start > end) {
            return ResponseEntity.badRequest().body(Map.of("error", "Το from είναι μετά το to."));
        }

        long began = System.nanoTime();
        refresher.refreshNow(start, end);
        if (refresher.hasPending()) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "error", "Η ανανέωση απέτυχε· δες τα logs. Θα ξαναδοκιμαστεί αυτόματα."));
        }
        return ResponseEntity.ok(Map.of(
                "from", from, "to", to,
                "ms", (System.nanoTime() - began) / 1_000_000,
                "message", "Τα συγκεντρωτικά ανανεώθηκαν."));
    }
}
