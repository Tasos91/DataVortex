package com.thesis.kafkacollector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Ελέγχει ότι το topic που ζήτησε ο χρήστης υπάρχει, πριν στείλουμε έστω και ένα μήνυμα.
 * <p>
 * Γιατί χρειάζεται: αν το topic δεν υπάρχει, ο producer μπλοκάρει μέχρι το {@code max.block.ms}
 * (60s by default) και μετά πετάει TimeoutException — δηλαδή το HTTP request κολλάει για ένα λάθος
 * που φαίνεται αμέσως. Εδώ αποτυγχάνουμε γρήγορα, με καθαρό μήνυμα.
 * <p>
 * Τα ονόματα των topics που υπάρχουν κρατιούνται σε cache για λίγο, ώστε να μη ρωτάμε
 * τον broker σε κάθε request.
 */
@Service
public class KafkaTopicService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicService.class);

    private final KafkaAdmin kafkaAdmin;
    private final Map<String, Long> knownTopics = new ConcurrentHashMap<>();

    /** Πόσο κρατάει η cache ενός topic που βρέθηκε (ms). */
    @Value("${spring.kafka.topic.cache-ttl-ms:30000}")
    private long cacheTtlMs;

    /** Partitions για topic που φτιάχνουμε εμείς, όταν το request δεν ορίζει αριθμό. */
    @Value("${spring.kafka.topic.default-partitions:6}")
    private int defaultPartitions;

    /** Replication factor· με ένα broker μπορεί να είναι μόνο 1. */
    @Value("${spring.kafka.topic.replication-factor:1}")
    private int replicationFactor;

    /** Το pattern στο οποίο είναι γραμμένος ο consumer αυτής της εφαρμογής. */
    @Value("${spring.kafka.topic.consumer-pattern:}")
    private String consumerPattern;

    @Value("${spring.kafka.topic.dlq:dlq.ingest}")
    private String dlqTopic;

    public KafkaTopicService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * Βεβαιώνεται ότι το topic μπορεί να χρησιμοποιηθεί.
     *
     * @param topic           το όνομα του topic
     * @param createIfMissing αν true και δεν υπάρχει, το δημιουργεί
     * @param partitions      πόσα partitions όταν το δημιουργεί (null = default)
     * @throws IllegalArgumentException αν δεν υπάρχει και δεν ζητήθηκε δημιουργία
     */
    public void ensureUsable(String topic, boolean createIfMissing, Integer partitions) {
        warnIfConsumerIgnores(topic);
        Long cachedAt = knownTopics.get(topic);
        if (cachedAt != null && System.currentTimeMillis() - cachedAt < cacheTtlMs) {
            return;
        }

        if (exists(topic)) {
            knownTopics.put(topic, System.currentTimeMillis());
            return;
        }

        if (!createIfMissing) {
            throw new IllegalArgumentException("Το topic '" + topic + "' δεν υπάρχει. Δημιούργησέ το πρώτα, "
                    + "ή στείλε \"createTopicIfMissing\": true (προαιρετικά και \"partitions\": N) στο request.");
        }

        int wanted = (partitions != null && partitions > 0) ? partitions : defaultPartitions;
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic)
                .partitions(wanted)
                .replicas(replicationFactor)
                .build());
        knownTopics.put(topic, System.currentTimeMillis());
        log.info("Δημιουργήθηκε το topic '{}' με {} partitions.", topic, wanted);
    }

    /**
     * Ο consumer γράφεται σε pattern (π.χ. {@code ingest\..*}), ώστε να πιάνει όσα topics
     * φτιάχνει ο χρήστης. Αν το topic του request δεν ταιριάζει, τα δεδομένα θα φτάσουν στο
     * Kafka αλλά δεν θα τα διαβάσει κανείς — αξίζει προειδοποίηση, όχι σφάλμα (μπορεί να το
     * καταναλώνει άλλη εφαρμογή).
     */
    private void warnIfConsumerIgnores(String topic) {
        if (consumerPattern == null || consumerPattern.isBlank() || topic.equals(dlqTopic)) {
            return;
        }
        try {
            if (!Pattern.matches(consumerPattern, topic)) {
                log.warn("Το topic '{}' δεν ταιριάζει με το pattern του consumer ('{}'): "
                        + "τα μηνύματα θα σταλούν, αλλά ο consumer αυτής της εφαρμογής δεν θα τα διαβάσει.",
                        topic, consumerPattern);
            }
        } catch (RuntimeException e) {
            log.warn("Άκυρο pattern consumer '{}': {}", consumerPattern, e.getMessage());
        }
    }

    private boolean exists(String topic) {
        try {
            return kafkaAdmin.describeTopics(topic).containsKey(topic);
        } catch (Exception e) {
            if (hasCause(e, UnknownTopicOrPartitionException.class)) {
                return false;
            }
            // Πρόβλημα επικοινωνίας με τον broker: δεν το μπερδεύουμε με "δεν υπάρχει το topic".
            throw new IllegalStateException("Αδύνατη η επικοινωνία με τον Kafka broker: " + e.getMessage(), e);
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) return true;
        }
        return false;
    }
}
