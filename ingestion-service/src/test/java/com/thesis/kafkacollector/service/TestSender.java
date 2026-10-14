package com.thesis.kafkacollector.service;

import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.ProducerFactory;

import java.util.List;

/**
 * Ένας {@link RecordSender} που δεν μιλά σε Kafka: κρατά ό,τι στάλθηκε, για να ελεγχθεί.
 * <p>
 * Χρησιμοποιεί τον {@code MockProducer} του ίδιου του Kafka, όχι δικό μας ψεύτικο: έτσι η
 * συμπεριφορά (σειρά, callbacks, flush) είναι αυτή του πραγματικού client.
 */
public final class TestSender extends RecordSender {

    private final MockProducer<String, IngestedRecord> producer;

    private TestSender(MockProducer<String, IngestedRecord> producer) {
        super(factoryOf(producer));
        this.producer = producer;
    }

    public static TestSender create() {
        // Στο Kafka 4.x ο σύντομος constructor έφυγε: θέλει ρητό partitioner. Ο RoundRobin κάνει
        // μια χαρά εδώ — στα tests μας ενδιαφέρει τι στάλθηκε, όχι σε ποιο partition κατέληξε.
        Serializer<String> keys = nullSerializer();
        Serializer<IngestedRecord> values = nullSerializer();
        return new TestSender(new MockProducer<>(true,
                new org.apache.kafka.clients.producer.RoundRobinPartitioner(), keys, values));
    }

    /** Τα μηνύματα που στάλθηκαν, με τη σειρά τους. */
    public List<ProducerRecord<String, IngestedRecord>> sent() {
        return producer.history();
    }

    public void clear() {
        producer.clear();
    }

    private static ProducerFactory<String, IngestedRecord> factoryOf(Producer<String, IngestedRecord> producer) {
        return new ProducerFactory<>() {
            @Override
            public Producer<String, IngestedRecord> createProducer() {
                return producer;
            }
        };
    }

    /** Ο MockProducer δεν σειριοποιεί τίποτα· κρατά τα αντικείμενα όπως είναι. */
    private static <T> Serializer<T> nullSerializer() {
        return (topic, data) -> null;
    }
}
