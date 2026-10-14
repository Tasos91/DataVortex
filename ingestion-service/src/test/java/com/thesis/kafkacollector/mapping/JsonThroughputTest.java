package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Πόσο κοστίζει η διαδρομή του JSON, σπασμένη στα κομμάτια της.
 * <p>
 * Το CSV μετρήθηκε και βελτιστοποιήθηκε· το JSON όχι ακόμα. Η υποψία είναι ότι η ανάλυση της
 * διαδρομής ({@code $.a.b.c}) γίνεται <b>ανά εγγραφή και ανά πεδίο</b>, δηλαδή το ίδιο σταθερό
 * κείμενο αναλύεται εκατομμύρια φορές.
 */
class JsonThroughputTest {

    private static final int ROWS = 200_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, String> MAPPINGS = new LinkedHashMap<>(Map.of(
            "cpu", "$.metrics.cpu",
            "latency_ms", "$.metrics.latency_ms",
            "mem_gb", "$.metrics.mem_gb",
            "errors", "$.metrics.errors",
            "throughput", "$.metrics.throughput",
            "host", "$.host"));

    private static JsonNode sample() throws Exception {
        return MAPPER.readTree("""
                {"ts": 1790000000000, "host": "node-07",
                 "metrics": {"cpu": 87.5, "latency_ms": 42.1, "mem_gb": 13.25,
                             "errors": 2, "throughput": 3286.9}}""");
    }

    @Test
    @DisplayName("ρυθμός: ανάλυση διαδρομών, μετατροπή σε Protobuf, και re-serialization")
    void throughput() throws Exception {
        JsonNode node = sample();

        // 1. Μόνο η ανάλυση των διαδρομών (6 πεδία + κλειδί + χρόνος ανά εγγραφή)
        for (int i = 0; i < 20_000; i++) {
            for (String path : MAPPINGS.values()) {
                JsonPathResolver.resolve(node, path);
            }
        }
        long started = System.nanoTime();
        int found = 0;
        for (int i = 0; i < ROWS; i++) {
            for (String path : MAPPINGS.values()) {
                if (JsonPathResolver.resolve(node, path) != null) {
                    found++;
                }
            }
        }
        long resolveMs = (System.nanoTime() - started) / 1_000_000;
        assertEquals(ROWS * MAPPINGS.size(), found);

        // 2. Ολόκληρη η μετατροπή σε Protobuf, χωρίς το ωμό payload
        for (int i = 0; i < 20_000; i++) {
            RecordMapper.map("perf", "", node, MAPPINGS);
        }
        started = System.nanoTime();
        int fields = 0;
        for (int i = 0; i < ROWS; i++) {
            IngestedRecord record = RecordMapper.map("perf", "", node, MAPPINGS);
            fields += record.getFieldsCount();
        }
        long mapMs = (System.nanoTime() - started) / 1_000_000;
        assertEquals(ROWS * MAPPINGS.size(), fields);

        // 3. Η επανασειριοποίηση του ωμού payload, που γίνεται σε ΚΑΘΕ εγγραφή
        for (int i = 0; i < 20_000; i++) {
            MAPPER.writeValueAsString(node);
        }
        started = System.nanoTime();
        int chars = 0;
        for (int i = 0; i < ROWS; i++) {
            chars += MAPPER.writeValueAsString(node).length();
        }
        long serializeMs = (System.nanoTime() - started) / 1_000_000;
        assertTrue(chars > 0);

        System.out.printf("ανάλυση διαδρομών (x%d/εγγραφή) : %,d εγγρ/s%n",
                MAPPINGS.size(), resolveMs > 0 ? ROWS * 1000L / resolveMs : 0);
        System.out.printf("JSON → Protobuf                : %,d εγγρ/s%n",
                mapMs > 0 ? ROWS * 1000L / mapMs : 0);
        System.out.printf("re-serialization ωμού payload  : %,d εγγρ/s%n",
                serializeMs > 0 ? ROWS * 1000L / serializeMs : 0);
    }
}
