package com.thesis.kafkacollector.service;

import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Η αποστολή στο Kafka, χωρίς να πληρώνουμε τίποτα περιττό ανά μήνυμα.
 * <p>
 * <b>Τι άλλαξε και γιατί:</b> το {@code KafkaTemplate.send(...)} επιστρέφει {@code CompletableFuture},
 * και ο κώδικας έβαζε από πάνω ένα {@code .whenComplete(...)} για να μετρήσει τις αποτυχίες. Αυτό
 * σημαίνει, <b>ανά μήνυμα</b>: δύο αντικείμενα {@code CompletableFuture}, μία λάμδα με δεσμευμένες
 * μεταβλητές, και ένα περιτύλιγμα μετρήσεων. Σε μια εισαγωγή 200.000 εγγραφών είναι περίπου
 * <b>600.000 αντικείμενα</b> που δημιουργούνται και αμέσως πετιούνται — δουλειά για τον συλλέκτη
 * σκουπιδιών, και μνήμη που δεν χρειαζόταν ποτέ να δεσμευτεί.
 * <p>
 * Εδώ χρησιμοποιείται ο {@link Producer} απευθείας, με την εκδοχή {@code send(record, callback)}.
 * Το callback είναι <b>ένα και το ίδιο</b> αντικείμενο για όλη την εισαγωγή: η {@link Session}.
 * Ένα αντικείμενο ανά <i>εισαγωγή</i>, αντί για τρία ανά <i>μήνυμα</i>.
 */
@Component
public class RecordSender {

    private static final Logger log = LoggerFactory.getLogger(RecordSender.class);
    /** Πόσα σφάλματα αποστολής καταγράφονται πριν σωπάσουμε· σε μαζική αποτυχία θα ήταν εκατομμύρια. */
    private static final int MAX_LOGGED_ERRORS = 5;

    private final ProducerFactory<String, IngestedRecord> factory;
    /**
     * Ο producer είναι βαρύ αντικείμενο (νήματα, συνδέσεις, buffer) και <b>ασφαλής για χρήση από
     * πολλά νήματα</b>: ένας για όλη την εφαρμογή είναι το σωστό. Το εργοστάσιο του Spring
     * επιστρέφει έτσι κι αλλιώς τον ίδιο, τυλιγμένο ώστε το close() να μην τον κλείνει στ' αλήθεια.
     */
    private volatile Producer<String, IngestedRecord> producer;

    public RecordSender(ProducerFactory<String, IngestedRecord> factory) {
        this.factory = factory;
    }

    private Producer<String, IngestedRecord> producer() {
        Producer<String, IngestedRecord> existing = producer;
        if (existing == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = factory.createProducer();
                }
                existing = producer;
            }
        }
        return existing;
    }

    /** Ανοίγει μια συνεδρία αποστολής. Μία ανά εισαγωγή. */
    public Session open(String topic) {
        return new Session(topic);
    }

    /**
     * Μια εισαγωγή σε εξέλιξη. Κρατά τον μετρητή αποτυχιών και είναι ταυτόχρονα το callback που
     * δίνεται στον producer — γι' αυτό δεν χρειάζεται νέο αντικείμενο ανά μήνυμα.
     */
    public final class Session implements Callback {

        private final String topic;
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicInteger logged = new AtomicInteger();
        private volatile String firstError;

        private Session(String topic) {
            this.topic = topic;
        }

        /**
         * @param eventTime ο χρόνος του γεγονότος σε epoch millis, ή {@code null} για «τώρα»
         * @param key       το κλειδί που καθορίζει το partition, ή {@code null} για μοίρασμα σε όλα
         */
        public void send(Long eventTime, String key, IngestedRecord value) {
            producer().send(new ProducerRecord<>(topic, null, eventTime, key, value), this);
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                return;
            }
            failed.incrementAndGet();
            if (firstError == null) {
                firstError = exception.getClass().getSimpleName() + ": " + exception.getMessage();
            }
            if (logged.incrementAndGet() <= MAX_LOGGED_ERRORS) {
                log.error("Αποτυχία αποστολής στο topic {}: {}", topic, exception.getMessage());
            }
        }

        public int failed() {
            return failed.get();
        }

        /** Το πρώτο σφάλμα, για να μπει στην απάντηση προς τον χρήστη. */
        public String firstError() {
            return firstError;
        }

        public String topic() {
            return topic;
        }
    }

    /**
     * Περιμένει να επιβεβαιώσει το Kafka ό,τι στάλθηκε. ΜΗΝ το αφαιρέσεις: χωρίς αυτό η απάντηση
     * «στάλθηκαν N» σημαίνει «μπήκαν στο buffer», όχι «είναι στο Kafka».
     */
    public void flush() {
        producer().flush();
    }
}
