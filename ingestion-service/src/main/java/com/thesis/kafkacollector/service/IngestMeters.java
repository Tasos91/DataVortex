package com.thesis.kafkacollector.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Το κοινό τέλος κάθε εισαγωγής, ανεξάρτητα από το αν ήρθε ως JSON, NDJSON ή CSV: περίμενε τα
 * acks του Kafka, μέτρησε, και πες τι έγινε.
 * <p>
 * Ζει σε δική του κλάση ώστε ο κανόνας «η απάντηση φεύγει <b>μετά</b> το flush» να είναι ένας και
 * μόνο, και να μην μπορεί να ξεχαστεί σε μια νέα διαδρομή εισαγωγής.
 */
@Component
public class IngestMeters {

    private static final Logger log = LoggerFactory.getLogger(IngestMeters.class);

    private final RecordSender sender;
    private final MeterRegistry registry;

    public IngestMeters(RecordSender sender, MeterRegistry registry) {
        this.sender = sender;
        this.registry = registry;
    }

    /**
     * Περιμένει να επιβεβαιώσει το Kafka ό,τι στάλθηκε, πριν απαντήσουμε στον client. Χωρίς αυτό η
     * απάντηση έλεγε «στάλθηκαν N» ενώ τα μηνύματα ήταν ακόμα στο buffer του producer: αν έπεφτε το
     * Kafka, ο client είχε 200 και τα δεδομένα δεν υπήρχαν πουθενά.
     */
    public IngestResult finish(String mode, int total, int read, int skipped, RecordSender.Session session) {
        return finish(session.topic(), mode, total, read, skipped, session.failed());
    }

    public IngestResult finish(String topic, String mode, int total, int read, int skipped, AtomicInteger failed) {
        return finish(topic, mode, total, read, skipped, failed.get());
    }

    private IngestResult finish(String topic, String mode, int total, int read, int skipped, int failedCount) {
        long started = System.nanoTime();
        sender.flush();
        int accepted = read - failedCount;
        IngestResult result = new IngestResult(total, accepted, skipped);

        Timer.builder("datavortex.ingest.flush").tag("mode", mode)
                .description("Αναμονή για τα acks του Kafka στο τέλος μιας εισαγωγής")
                .register(registry)
                .record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        count(mode, "accepted", accepted);
        count(mode, "failed", result.failed());
        count(mode, "skipped", skipped);

        if (result.failed() > 0 || skipped > 0) {
            log.warn("{}: {} εγγραφές, {} στο Kafka, {} απέτυχαν, {} άκυρες (topic {})",
                    mode, total, accepted, result.failed(), skipped, topic);
        } else {
            log.info("{}: {} μηνύματα στο topic {}", mode, accepted, topic);
        }
        return result;
    }

    private void count(String mode, String result, int amount) {
        if (amount > 0) {
            Counter.builder("datavortex.ingest.records")
                    .description("Εγγραφές που μπήκαν στον σωλήνα, ανά τρόπο εισαγωγής και αποτέλεσμα")
                    .tags("mode", mode, "result", result)
                    .register(registry)
                    .increment(amount);
        }
    }
}
