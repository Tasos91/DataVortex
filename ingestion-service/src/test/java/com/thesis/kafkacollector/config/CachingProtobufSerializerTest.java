package com.thesis.kafkacollector.config;

import com.google.protobuf.Value;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Η απόδοση δεν αξίζει τίποτα αν τα bytes δεν είναι τα ίδια. Εδώ ελέγχεται ακριβώς αυτό:
 * byte-προς-byte ταύτιση με τον serializer του Confluent, και ότι ο κανονικός deserializer
 * διαβάζει ό,τι γράφουμε.
 */
class CachingProtobufSerializerTest {

    private static final Map<String, Object> CONFIG = Map.of(
            "schema.registry.url", "mock://caching-test", "auto.register.schemas", true);

    private static IngestedRecord record(double cpu) {
        return IngestedRecord.newBuilder()
                .setSourceId("perf")
                .putFields("host", Value.newBuilder().setStringValue("node-07").build())
                .putFields("cpu", Value.newBuilder().setNumberValue(cpu).build())
                .putFields("errors", Value.newBuilder().setNumberValue(0).build())
                .build();
    }

    @Test
    @DisplayName("βγάζει ΑΚΡΙΒΩΣ τα ίδια bytes με τον serializer του Confluent")
    void identicalBytes() {
        try (var confluent = new KafkaProtobufSerializer<IngestedRecord>();
             var fast = new CachingProtobufSerializer()) {
            confluent.configure(CONFIG, false);
            fast.configure(CONFIG, false);

            for (int i = 0; i < 5; i++) {
                IngestedRecord r = record(80 + i);
                assertArrayEquals(confluent.serialize("ingest.same", r), fast.serialize("ingest.same", r),
                        "διαφορετικά bytes στην επανάληψη " + i);
            }
        }
    }

    @Test
    @DisplayName("ο κανονικός deserializer διαβάζει ό,τι γράφουμε")
    void roundTrip() {
        try (var fast = new CachingProtobufSerializer();
             var de = new KafkaProtobufDeserializer<IngestedRecord>()) {
            fast.configure(CONFIG, false);
            de.configure(Map.of("schema.registry.url", "mock://caching-test",
                    "specific.protobuf.value.type", IngestedRecord.class.getName()), false);

            IngestedRecord original = record(87.5);
            IngestedRecord back = de.deserialize("ingest.rt", fast.serialize("ingest.rt", original));

            assertEquals(original, back);
            assertEquals(87.5, back.getFieldsOrThrow("cpu").getNumberValue());
            assertEquals("node-07", back.getFieldsOrThrow("host").getStringValue());
        }
    }

    @Test
    @DisplayName("κάθε topic έχει δικό του σχήμα και δικό του prefix")
    void prefixIsPerTopic() {
        try (var confluent = new KafkaProtobufSerializer<IngestedRecord>();
             var fast = new CachingProtobufSerializer()) {
            confluent.configure(CONFIG, false);
            fast.configure(CONFIG, false);

            IngestedRecord r = record(50);
            for (String topic : new String[]{"ingest.a", "ingest.b", "ingest.a"}) {
                assertArrayEquals(confluent.serialize(topic, r), fast.serialize(topic, r), topic);
            }
        }
    }

    @Test
    @DisplayName("το null μένει null (ο producer το χρειάζεται για tombstones)")
    void nullStaysNull() {
        try (var fast = new CachingProtobufSerializer()) {
            fast.configure(CONFIG, false);
            assertNull(fast.serialize("ingest.n", null));
        }
    }

    @Test
    @DisplayName("ρυθμός: πόσο κερδίζουμε στην πράξη")
    void throughput() {
        IngestedRecord r = record(87.5);
        int n = 200_000;
        long confluentMs;
        long fastMs;

        try (var confluent = new KafkaProtobufSerializer<IngestedRecord>()) {
            confluent.configure(CONFIG, false);
            for (int i = 0; i < 20_000; i++) confluent.serialize("ingest.bench", r);
            long started = System.nanoTime();
            for (int i = 0; i < n; i++) confluent.serialize("ingest.bench", r);
            confluentMs = (System.nanoTime() - started) / 1_000_000;
        }
        try (var fast = new CachingProtobufSerializer()) {
            fast.configure(CONFIG, false);
            for (int i = 0; i < 20_000; i++) fast.serialize("ingest.bench", r);
            long started = System.nanoTime();
            for (int i = 0; i < n; i++) fast.serialize("ingest.bench", r);
            fastMs = (System.nanoTime() - started) / 1_000_000;
        }

        System.out.printf("Confluent : %,d/s%nμε cache  : %,d/s  (×%.1f)%n",
                confluentMs > 0 ? n * 1000L / confluentMs : 0,
                fastMs > 0 ? n * 1000L / fastMs : 0,
                confluentMs / (double) Math.max(1, fastMs));
        assertTrue(fastMs < confluentMs, "η cached διαδρομή πρέπει να είναι ταχύτερη");
    }
}
