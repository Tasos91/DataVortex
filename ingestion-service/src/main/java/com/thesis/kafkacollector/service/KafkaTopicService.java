package com.thesis.kafkacollector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
    /** Φτιάχνεται μόνο όταν χρειαστεί (δημιουργία νέου topic) και κλείνει στο shutdown. */
    private volatile AdminClient adminClient;
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

    /** Το topic όταν το request δεν ορίζει δικό του. */
    @Value("${spring.kafka.topic.ingestion:ingest.default}")
    private String defaultTopic;

    /** Ο group του consumer· χρειάζεται για να ελεγχθεί ότι ανέλαβε το νέο topic. */
    @Value("${spring.cloud.stream.bindings.consumeData-in-0.group:}")
    private String consumerGroup;

    /** Πόσο περιμένουμε την ανάθεση ενός νέου topic στον consumer, πριν στείλουμε. */
    @Value("${datavortex.assignment-wait-ms:30000}")
    private long assignmentWaitMs;

    public KafkaTopicService(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    /**
     * Το topic του request (ή το default) αφού επιβεβαιωθεί ότι μπορεί να χρησιμοποιηθεί.
     * Ο έλεγχος γίνεται ΠΡΙΝ σταλεί το πρώτο μήνυμα, ώστε ένα λάθος όνομα να γίνεται αμέσως 400
     * και να μη κολλάει ο producer περιμένοντας metadata που δεν θα έρθουν ποτέ.
     */
    public String resolve(String requested, boolean createIfMissing, Integer partitions) {
        String topic = requested != null && !requested.isBlank() ? requested.strip() : defaultTopic;
        ensureUsable(topic, createIfMissing, partitions);
        return topic;
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
        awaitConsumerAssignment(topic);
    }

    /**
     * Περιμένει ο consumer group να αναθέσει το <b>νέο</b> topic, πριν σταλεί το πρώτο μήνυμα.
     * <p>
     * <b>ΜΗΝ ΤΟ ΑΦΑΙΡΕΣΕΙΣ.</b> Χωρίς αυτό υπήρχε σιωπηλή απώλεια δεδομένων, μετρημένη: ο consumer
     * ακούει σε <i>pattern</i> και ανακαλύπτει νέα topics μόνο στο επόμενο metadata refresh. Μέχρι
     * τότε τα μηνύματα φεύγουν κανονικά, ο client παίρνει <b>200 «960 από 960 εγγραφές»</b>, και όταν
     * ο consumer τελικά αναθέσει τα partitions ξεκινά από το <b>τέλος</b> τους: τα 960 δεν
     * διαβάζονται ποτέ, δεν πάνε ούτε στο DLQ, και κανένα σφάλμα δεν εμφανίζεται πουθενά.
     * <p>
     * Κοστίζει μόνο στην πρώτη εισαγωγή κάθε νέου topic — μετά, το topic υπάρχει και ο δρόμος αυτός
     * δεν εκτελείται καθόλου. Αν ο consumer δεν προλάβει μέσα στο χρονικό όριο, προχωράμε με ρητή
     * προειδοποίηση: καλύτερα να σταλούν τα δεδομένα παρά να μπλοκάρει η εισαγωγή για πάντα (μπορεί
     * ο consumer να τρέχει αλλού ή να είναι σβηστός).
     */
    private void awaitConsumerAssignment(String topic) {
        if (assignmentWaitMs <= 0 || consumerGroup == null || consumerGroup.isBlank()) {
            return;
        }
        long deadline = System.currentTimeMillis() + assignmentWaitMs;
        long started = System.nanoTime();
        while (System.currentTimeMillis() < deadline) {
            if (isAssigned(topic)) {
                log.info("Ο consumer ανέλαβε το '{}' σε {} ms· η αποστολή μπορεί να ξεκινήσει.",
                        topic, (System.nanoTime() - started) / 1_000_000);
                return;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("Ο consumer group '{}' δεν ανέλαβε το topic '{}' σε {} ms. Τα μηνύματα θα σταλούν, "
                        + "αλλά όσα φτάσουν πριν την ανάθεση μπορεί να μη διαβαστούν ποτέ.",
                consumerGroup, topic, assignmentWaitMs);
    }

    /**
     * Έχει κάποιο μέλος του group ανατεθειμένο έστω ένα partition αυτού του topic;
     * <p>
     * Ο AdminClient φτιάχνεται τεμπέλικα και κρατιέται: η διαδρομή αυτή εκτελείται μόνο όταν
     * δημιουργείται νέο topic, αλλά τότε καλείται κάθε 250 ms.
     */
    private boolean isAssigned(String topic) {
        try {
            var description = admin().describeConsumerGroups(java.util.List.of(consumerGroup))
                    .all().get(5, TimeUnit.SECONDS).get(consumerGroup);
            if (description == null) {
                return false;
            }
            for (var member : description.members()) {
                for (var tp : member.assignment().topicPartitions()) {
                    if (topic.equals(tp.topic())) {
                        return true;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.debug("Ο έλεγχος ανάθεσης για το '{}' απέτυχε: {}", topic, e.getMessage());
        }
        return false;
    }

    private AdminClient admin() {
        AdminClient existing = adminClient;
        if (existing == null) {
            synchronized (this) {
                if (adminClient == null) {
                    adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
                }
                existing = adminClient;
            }
        }
        return existing;
    }

    @jakarta.annotation.PreDestroy
    public void closeAdmin() {
        AdminClient existing = adminClient;
        if (existing != null) {
            existing.close(java.time.Duration.ofSeconds(2));
        }
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
