package com.thesis.kafkacollector.service;

import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.csv.CsvReader;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Πόσο γρήγορα γίνεται μια γραμμή CSV μήνυμα Protobuf, <b>χωρίς</b> Kafka στη μέση.
 * <p>
 * Χωρίζει τη μέτρηση σε δύο κομμάτια, γιατί μόνο έτσι ξέρεις πού να δουλέψεις: ο αναγνώστης του
 * CSV από μόνος του, και η πλήρης διαδρομή μέχρι το έτοιμο μήνυμα. Ό,τι απομένει στο πραγματικό
 * σύστημα είναι η σειριοποίηση και ο producer — δηλαδή το Kafka, όχι ο κώδικάς μας.
 */
class CsvThroughputTest {

    private static final int ROWS = 200_000;

    private static String csv() {
        StringBuilder sb = new StringBuilder(ROWS * 70);
        sb.append("ts,host,cpu,latency_ms,mem_gb,errors,throughput\n");
        long t = 1_790_000_000_000L;
        for (int i = 0; i < ROWS; i++) {
            sb.append(t + i * 10L).append(",node-").append(i % 12)
              .append(",87.5").append(",42.1").append(",13.25").append(",2").append(",3286.9\n");
        }
        return sb.toString();
    }

    private static CsvPlan plan() {
        return new CsvPlan("perf", "ingest.perf", false, 1, ",", ".", true, "ts", null, "host",
                List.of(new CsvPlan.Column("host", "host", "text"),
                        new CsvPlan.Column("cpu", "cpu", "number"),
                        new CsvPlan.Column("latency_ms", "latency_ms", "number"),
                        new CsvPlan.Column("mem_gb", "mem_gb", "number"),
                        new CsvPlan.Column("errors", "errors", "number"),
                        new CsvPlan.Column("throughput", "throughput", "number")),
                false);
    }

    /** Μετράει μηνύματα και τα πετάει: εδώ μας ενδιαφέρει το κόστος της μετατροπής. */
    private static final class CountingTemplate extends KafkaTemplate<String, IngestedRecord> {
        final AtomicLong sent = new AtomicLong();
        final AtomicLong fields = new AtomicLong();

        CountingTemplate() {
            super(new DefaultKafkaProducerFactory<>(Map.of()));
        }

        @Override
        public CompletableFuture<SendResult<String, IngestedRecord>> send(
                ProducerRecord<String, IngestedRecord> record) {
            sent.incrementAndGet();
            fields.addAndGet(record.value().getFieldsCount());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void flush() {
        }
    }

    private static final class NoopTopics extends KafkaTopicService {
        NoopTopics() {
            super(null);
        }

        @Override
        public String resolve(String requested, boolean createIfMissing, Integer partitions) {
            return requested;
        }
    }

    @Test
    @DisplayName("ρυθμός: ανάγνωση CSV μόνη της, και ολόκληρη η μετατροπή σε Protobuf")
    void throughput() throws Exception {
        String csv = csv();

        long started = System.nanoTime();
        int rows = 0;
        try (CsvReader r = new CsvReader(new StringReader(csv), ',')) {
            while (r.next() != null) {
                rows++;
            }
        }
        long readMs = (System.nanoTime() - started) / 1_000_000;
        assertEquals(ROWS + 1, rows, "οι γραμμές συν η επικεφαλίδα");

        CountingTemplate template = new CountingTemplate();
        CsvIngestService service = new CsvIngestService(template, new NoopTopics(),
                new IngestMeters(template, new SimpleMeterRegistry()));

        started = System.nanoTime();
        IngestResult result = service.ingest(new StringReader(csv), plan());
        long mapMs = (System.nanoTime() - started) / 1_000_000;

        assertEquals(ROWS, result.read());
        assertEquals(ROWS, template.sent.get());
        assertEquals(6L * ROWS, template.fields.get(), "έξι πεδία ανά γραμμή");

        System.out.printf("ανάγνωση CSV   : %d γραμμές σε %d ms (%,d/s)%n",
                ROWS, readMs, readMs > 0 ? ROWS * 1000L / readMs : 0);
        System.out.printf("CSV → Protobuf : %d γραμμές σε %d ms (%,d/s)%n",
                ROWS, mapMs, mapMs > 0 ? ROWS * 1000L / mapMs : 0);
    }
}
