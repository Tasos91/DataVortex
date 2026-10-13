package com.thesis.kafkacollector.config;

import com.thesis.kafkacollector.proto.IngestedRecordProto;
import com.thesis.kafkacollector.service.DashboardMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.List;
import java.util.function.Consumer;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private final DashboardMetricsService metricsService;

    public KafkaConsumerConfig(DashboardMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    /**
     * Batch consumer: κάθε κλήση παίρνει ό,τι έφερε ένα poll (μέχρι {@code max.poll.records}),
     * όχι ένα μήνυμα τη φορά. Αυτό είναι η προϋπόθεση για batch inserts στη βάση — με ένα
     * μήνυμα τη φορά θα είχαμε ένα round-trip στη βάση ανά record.
     * <p>
     * Παίρνουμε {@code Message<List<...>>} και όχι σκέτη {@code List<...>} γιατί χρειαζόμαστε
     * τα metadata του Kafka (topic, partition, offset, timestamp) από τα headers: είναι το
     * κλειδί με το οποίο η εγγραφή στη βάση γίνεται idempotent.
     */
    @Bean
    public Consumer<Message<List<IngestedRecordProto.IngestedRecord>>> consumeData() {
        return message -> {
            List<IngestedRecordProto.IngestedRecord> records = message.getPayload();
            if (records.isEmpty()) {
                return;
            }

            MessageHeaders headers = message.getHeaders();
            List<?> topics = asList(headers.get(KafkaHeaders.RECEIVED_TOPIC));
            List<?> partitions = asList(headers.get(KafkaHeaders.RECEIVED_PARTITION));
            List<?> offsets = asList(headers.get(KafkaHeaders.OFFSET));

            String lastPayload = null;
            int counted = 0;
            for (int i = 0; i < records.size(); i++) {
                IngestedRecordProto.IngestedRecord record = records.get(i);
                if (record == null) {
                    continue; // δεν θα έπρεπε να συμβεί: τα χαλασμένα πάνε στο DLQ
                }
                // Debug και όχι info: σε υψηλό throughput μία γραμμή ανά μήνυμα σκοτώνει το I/O.
                if (log.isDebugEnabled()) {
                    log.debug("Record από {}-{} offset {}: sourceId {}, lat {}, πεδία {}",
                            at(topics, i), at(partitions, i), at(offsets, i),
                            record.getSourceId(), record.getLat(), record.getFieldsMap());
                }
                lastPayload = record.getPayloadJson();
                counted++;
            }

            metricsService.recordMessages(counted, lastPayload);
            log.info("Batch: {} μηνύματα από {} (τελευταίο offset {})",
                    counted, at(topics, 0), at(offsets, records.size() - 1));
        };
    }

    private static List<?> asList(Object header) {
        return (header instanceof List<?> list) ? list : List.of();
    }

    private static Object at(List<?> list, int index) {
        return (index >= 0 && index < list.size()) ? list.get(index) : "?";
    }
}
