package com.thesis.kafkacollector.mapping;

import com.google.protobuf.Value;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Πόσο κοστίζει η σειριοποίηση ενός μηνύματος μέσω Schema Registry.
 * <p>
 * Το μετράμε επειδή οι μετρήσεις έδειξαν ότι ο producer είναι το στενό σημείο του σωλήνα, και ότι
 * ΔΕΝ είναι ούτε το batching ούτε η συμπίεση: ο χρόνος μένει ίδιος όσο κι αν τα αλλάξεις, άρα
 * υπάρχει σταθερό κόστος ανά μήνυμα. Εδώ φαίνεται πόσο από αυτό είναι η σειριοποίηση και πόσο
 * μένει στον ίδιο τον producer (accumulator, κλειδώματα, δίκτυο, broker).
 */
class SerializerThroughputTest {

    private static IngestedRecord record() {
        return IngestedRecord.newBuilder()
                .setSourceId("perf")
                .putFields("host", Value.newBuilder().setStringValue("node-07").build())
                .putFields("cpu", Value.newBuilder().setNumberValue(87.5).build())
                .putFields("latency_ms", Value.newBuilder().setNumberValue(42.1).build())
                .putFields("mem_gb", Value.newBuilder().setNumberValue(13.25).build())
                .putFields("errors", Value.newBuilder().setNumberValue(2).build())
                .putFields("throughput", Value.newBuilder().setNumberValue(3286.9).build())
                .build();
    }

    @Test
    @DisplayName("ρυθμός σειριοποίησης Protobuf, με και χωρίς το περιτύλιγμα του Schema Registry")
    void throughput() throws Exception {
        IngestedRecord record = record();
        int n = 200_000;

        // 1. Σκέτο Protobuf, χωρίς registry: το κάτω όριο του δυνατού.
        for (int i = 0; i < 20_000; i++) {
            record.toByteArray();
        }
        long started = System.nanoTime();
        int bytes = 0;
        for (int i = 0; i < n; i++) {
            bytes += record.toByteArray().length;
        }
        long plainMs = (System.nanoTime() - started) / 1_000_000;

        // 2. Μέσω KafkaProtobufSerializer (mock registry: καμία κλήση δικτύου μετά την πρώτη).
        int wrapped;
        long schemaMs;
        try (KafkaProtobufSerializer<IngestedRecord> ser = new KafkaProtobufSerializer<>()) {
            ser.configure(Map.of("schema.registry.url", "mock://throughput",
                    "auto.register.schemas", true), false);
            for (int i = 0; i < 20_000; i++) {
                ser.serialize("ingest.perf", record);
            }
            started = System.nanoTime();
            int acc = 0;
            for (int i = 0; i < n; i++) {
                acc += ser.serialize("ingest.perf", record).length;
            }
            schemaMs = (System.nanoTime() - started) / 1_000_000;
            wrapped = acc / n;
        }

        System.out.printf("Protobuf σκέτο        : %,d/s  (%d bytes)%n",
                plainMs > 0 ? n * 1000L / plainMs : 0, bytes / n);
        System.out.printf("μέσω Schema Registry  : %,d/s  (%d bytes)%n",
                schemaMs > 0 ? n * 1000L / schemaMs : 0, wrapped);
        assertTrue(wrapped > bytes / n, "το περιτύλιγμα προσθέτει το schema id");
    }
}
