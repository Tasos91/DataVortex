package com.thesis.kafkacollector.config;

import com.thesis.kafkacollector.service.KafkaTopicService;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Τι γίνεται με ένα μήνυμα που δεν μπορεί να επεξεργαστεί (poison pill).
 * <p>
 * Χωρίς αυτό, ένα χαλασμένο μήνυμα (π.χ. παλιά raw bytes χωρίς schema id) ξαναδιαβάζεται
 * ατέρμονα στο ίδιο offset και η ροή σταματά για πάντα. Εδώ: λίγες προσπάθειες, και μετά
 * το μήνυμα πάει αυτούσιο σε ένα topic DLQ και ο consumer συνεχίζει.
 * <p>
 * Το DLQ topic είναι έξω από το pattern του consumer, αλλιώς θα το ξαναδιάβαζε.
 */
@Configuration
public class KafkaErrorHandlingConfig implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic.dlq:dlq.ingest}")
    private String dlqTopic;

    /** Πόσες φορές ξαναδοκιμάζει πριν στείλει στο DLQ. */
    @Value("${spring.kafka.consumer.retry-attempts:2}")
    private long retryAttempts;

    @Value("${spring.kafka.consumer.retry-interval-ms:1000}")
    private long retryIntervalMs;

    /**
     * Ξεχωριστός producer με byte[] serializers: το χαλασμένο μήνυμα προωθείται όπως ήρθε,
     * χωρίς να προσπαθήσει κανείς να το κάνει Protobuf (που είναι και ο λόγος που απέτυχε).
     * <p>
     * ΔΕΝ είναι bean επίτηδες: ένα δεύτερο KafkaTemplate/ProducerFactory στο context κάνει
     * το KafkaAutoConfiguration να μην φτιάξει το κανονικό (είναι ConditionalOnMissingBean).
     */
    private DefaultKafkaProducerFactory<byte[], byte[]> dlqProducerFactory;

    @Bean
    public CommonErrorHandler dlqErrorHandler(KafkaTopicService topicService) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        this.dlqProducerFactory = new DefaultKafkaProducerFactory<>(config);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                new KafkaTemplate<>(dlqProducerFactory),
                (record, exception) -> {
                    // Το DLQ topic δεν δημιουργείται αυτόματα από τον broker, το φτιάχνουμε εμείς.
                    topicService.ensureUsable(dlqTopic, true, 1);
                    log.error("Μήνυμα στο DLQ {}: από {}-{} offset {}. Αιτία: {}",
                            dlqTopic, record.topic(), record.partition(), record.offset(),
                            exception.getMessage());
                    // partition -1: ας διαλέξει το Kafka, το DLQ δεν έχει τα ίδια partitions
                    return new TopicPartition(dlqTopic, -1);
                });
        // Τα αρχικά topic/partition/offset και το σφάλμα μπαίνουν σε headers του μηνύματος DLQ.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(retryIntervalMs, retryAttempts));
    }

    /**
     * Ο binder του Spring Cloud Stream φτιάχνει μόνος του τα listener containers·
     * εδώ τους περνάμε τον error handler.
     */
    @Override
    public void destroy() {
        if (dlqProducerFactory != null) {
            dlqProducerFactory.destroy();
        }
    }

    @Bean
    public ListenerContainerCustomizer<AbstractMessageListenerContainer<?, ?>> containerCustomizer(
            CommonErrorHandler dlqErrorHandler) {
        return (container, destinationName, group) -> container.setCommonErrorHandler(dlqErrorHandler);
    }
}
