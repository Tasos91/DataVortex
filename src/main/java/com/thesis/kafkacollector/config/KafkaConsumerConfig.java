package com.thesis.kafkacollector.config;

import com.thesis.kafkacollector.proto.IngestedRecordProto;
import com.thesis.kafkacollector.service.DashboardMetricsService;
import com.thesis.kafkacollector.service.IngestedRecordWriter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private final DashboardMetricsService metricsService;
    private final IngestedRecordWriter writer;
    private final DistributionSummary batchSize;
    private final Counter consumedOk;
    private final Counter consumedUnreadable;

    public KafkaConsumerConfig(DashboardMetricsService metricsService, IngestedRecordWriter writer,
                               MeterRegistry registry) {
        this.metricsService = metricsService;
        this.writer = writer;
        this.batchSize = DistributionSummary.builder("datavortex.consume.batch.size")
                .description("Μηνύματα ανά poll· αν είναι μικρό, τα inserts είναι πολλά και μικρά")
                .register(registry);
        this.consumedOk = Counter.builder("datavortex.consume.records").tag("result", "ok").register(registry);
        this.consumedUnreadable = Counter.builder("datavortex.consume.records")
                .tag("result", "unreadable").register(registry);
    }

    /**
     * Batch consumer: κάθε κλήση παίρνει ό,τι έφερε ένα poll (μέχρι {@code max.poll.records}),
     * όχι ένα μήνυμα τη φορά. Όλο το batch γράφεται στη βάση με ένα INSERT.
     * <p>
     * Παίρνουμε {@code Message<List<...>>} και όχι σκέτη {@code List<...>} γιατί χρειαζόμαστε
     * τα metadata του Kafka (topic, partition, offset, timestamp) από τα headers: είναι το
     * κλειδί με το οποίο η εγγραφή στη βάση γίνεται idempotent.
     * <p>
     * Αν η εγγραφή αποτύχει, η εξαίρεση ανεβαίνει: ο error handler ξαναδοκιμάζει το batch
     * (και δεν γίνεται commit των offsets), άρα δεν χάνονται δεδομένα.
     * <p>
     * Ο τύπος είναι {@code List<Object>} και όχι {@code List<IngestedRecord>} επίτηδες: ένα
     * μήνυμα που δεν διαβάστηκε (ErrorHandlingDeserializer) φτάνει εδώ ως {@code KafkaNull},
     * και με δηλωμένο τύπο record θα έσκαγε με ClassCastException — στέλνοντας ΟΛΟ το batch
     * (500 καλά μηνύματα) στο DLQ. Τώρα γράφουμε τα καλά και στέλνουμε στο DLQ μόνο το ένα
     * χαλασμένο, μέσω BatchListenerFailedException με τη θέση του.
     */
    @Bean
    public Consumer<Message<List<Object>>> consumeData() {
        return message -> {
            List<Object> records = message.getPayload();
            if (records.isEmpty()) {
                return;
            }

            MessageHeaders headers = message.getHeaders();
            List<?> topics = asList(headers.get(KafkaHeaders.RECEIVED_TOPIC));
            List<?> partitions = asList(headers.get(KafkaHeaders.RECEIVED_PARTITION));
            List<?> offsets = asList(headers.get(KafkaHeaders.OFFSET));
            List<?> timestamps = asList(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP));

            List<IngestedRecordWriter.Row> rows = new ArrayList<>(records.size());
            String lastPayload = null;
            int badIndex = -1;

            for (int i = 0; i < records.size(); i++) {
                if (!(records.get(i) instanceof IngestedRecordProto.IngestedRecord record)) {
                    if (badIndex < 0) {
                        badIndex = i; // το πρώτο χαλασμένο: αυτό θα πάει στο DLQ
                    }
                    consumedUnreadable.increment();
                    log.error("Μη αναγνώσιμο μήνυμα από {}-{} offset {}: πάει στο DLQ.",
                            at(topics, i), at(partitions, i), at(offsets, i));
                    continue;
                }
                rows.add(new IngestedRecordWriter.Row(
                        longAt(timestamps, i, System.currentTimeMillis()),
                        String.valueOf(at(topics, i)),
                        (int) longAt(partitions, i, 0),
                        longAt(offsets, i, 0),
                        record.getSourceId(),
                        // optional στο proto: null όταν ο χρήστης δεν χαρτογράφησε συντεταγμένες
                        record.hasLat() ? record.getLat() : null,
                        record.hasLon() ? record.getLon() : null,
                        IngestedRecordWriter.fieldsToJson(record),
                        record.getPayloadJson().isEmpty() ? null : record.getPayloadJson()));
                lastPayload = record.getPayloadJson();

                // Debug και όχι info: σε υψηλό throughput μία γραμμή ανά μήνυμα σκοτώνει το I/O.
                if (log.isDebugEnabled()) {
                    log.debug("Record από {}-{} offset {}: sourceId {}, πεδία {}",
                            at(topics, i), at(partitions, i), at(offsets, i),
                            record.getSourceId(), record.getFieldsMap());
                }
            }

            // Πρώτα γράφουμε τα καλά, και μόνο μετά σηματοδοτούμε το χαλασμένο: έτσι δεν
            // χάνεται τίποτα, και η επανάληψη των υπόλοιπων είναι ακίνδυνη (idempotent insert).
            int written = writer.write(rows);
            batchSize.record(records.size());
            consumedOk.increment(rows.size());
            metricsService.recordMessages(rows.size(), lastPayload);
            log.info("Batch: {} μηνύματα από {}, γράφτηκαν {} (διπλά: {})",
                    rows.size(), at(topics, 0), written, rows.size() - written);

            if (badIndex >= 0) {
                throw new BatchListenerFailedException("Μη αναγνώσιμο μήνυμα στο batch", badIndex);
            }
        };
    }

    private static List<?> asList(Object header) {
        return (header instanceof List<?> list) ? list : List.of();
    }

    private static Object at(List<?> list, int index) {
        return (index >= 0 && index < list.size()) ? list.get(index) : "?";
    }

    private static long longAt(List<?> list, int index, long fallback) {
        Object value = at(list, index);
        return (value instanceof Number number) ? number.longValue() : fallback;
    }
}
