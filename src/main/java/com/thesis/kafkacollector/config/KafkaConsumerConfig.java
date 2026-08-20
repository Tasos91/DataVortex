package com.thesis.kafkacollector.config;

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
    public Consumer<String> consumeData() {
        return message -> {
            log.info("✅ ΕΠΙΤΕΛΟΥΣ! Το Spring Cloud Stream διάβασε: {}", message);
            metricsService.recordMessage(message);
        };
    }

}