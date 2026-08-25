package com.thesis.kafkacollector.config;

import com.thesis.kafkacollector.proto.IngestedRecordProto;
import com.thesis.kafkacollector.service.DashboardMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.function.Consumer;

@Configuration
public class KafkaConsumerConfig {

    // Σχολιάζουμε το Repository και τον ObjectMapper αφού δε θα τα χρησιμοποιήσουμε τώρα
    // private final IngestedRecordRepository repository;
    // private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);
    private final DashboardMetricsService metricsService;

    // Στον constructor κρατάμε ΜΟΝΟ το metricsService
    public KafkaConsumerConfig(DashboardMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Bean
    public Consumer<byte[]> consumeData() {
        return payload -> {
            try {
                // Η ΜΑΓΕΙΑ: Μετατρέπουμε τα "γυμνά" bytes ξανά σε Protobuf Object
                IngestedRecordProto.IngestedRecord record = IngestedRecordProto.IngestedRecord.parseFrom(payload);

                log.info("✅ ΕΠΙΤΕΛΟΥΣ! Το Spring Cloud Stream διάβασε Protobuf. SourceID: {}, Lat: {}",
                        record.getSourceId(), record.getLat());

                // ΠΡΟΣΟΧΗ: Αν η recordMessage θέλει String, στείλε της π.χ. το JSON string που κρύψαμε μέσα στο record
                metricsService.recordMessage(record.getPayloadJson());

            } catch (Exception e) {
                log.error("❌ Αποτυχία μετατροπής των bytes σε Protobuf", e);
            }
        };
    }

}