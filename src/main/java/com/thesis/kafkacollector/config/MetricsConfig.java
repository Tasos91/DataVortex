package com.thesis.kafkacollector.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Μετρικές για Prometheus/Grafana.
 * <p>
 * Η σημαντικότερη είναι το <b>consumer lag</b>: πόσα μηνύματα έχουν γραφτεί στο Kafka και δεν
 * τα έχει διαβάσει ακόμα ο consumer. Αν ανεβαίνει σταθερά, η αποθήκευση δεν προλαβαίνει την
 * εισαγωγή — κανένα άλλο νούμερο δεν το δείχνει αυτό.
 */
@Configuration
@EnableScheduling
public class MetricsConfig {

    /** Κοινά tags, ώστε στο Grafana να ξεχωρίζει ποιο service έστειλε τη μετρική. */
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> commonTags(
            @Value("${spring.application.name:datavortex}") String application) {
        return registry -> registry.config()
                .commonTags("application", application)
                // Ιστογράμματα μόνο εκεί που μας νοιάζει το p99, όχι σε όλα (κοστίζουν σε μνήμη)
                .meterFilter(MeterFilter.maxExpected("datavortex.db.insert", java.time.Duration.ofSeconds(10)));
    }

    /**
     * Υπολογίζει το lag με AdminClient αντί να βασιστεί στις μετρικές του Kafka client:
     * έτσι φαίνεται ανά topic, δουλεύει ακόμα κι όταν ο consumer είναι σταματημένος,
     * και είναι το ίδιο νούμερο που δείχνει το {@code kafka-consumer-groups --describe}.
     */
    @Component
    static class ConsumerLagMetrics implements DisposableBean {

        private static final Logger log = LoggerFactory.getLogger(ConsumerLagMetrics.class);

        private final AdminClient admin;
        private final MultiGauge lagPerTopic;
        private final Map<String, Long> totals = new ConcurrentHashMap<>();
        private final String groupId;
        private final java.util.regex.Pattern consumerPattern;

        ConsumerLagMetrics(KafkaAdmin kafkaAdmin, MeterRegistry registry,
                           @Value("${CONSUMER_GROUP:default-ingestion-group}") String groupId,
                           @Value("${spring.kafka.topic.consumer-pattern:.*}") String pattern) {
            this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
            this.groupId = groupId;
            // Παλιά committed offsets για topics που δεν ακούει πια ο consumer θα έδειχναν
            // μόνιμο lag και θα χτυπούσαν συναγερμό για το τίποτα.
            this.consumerPattern = java.util.regex.Pattern.compile(pattern.isBlank() ? ".*" : pattern);
            this.lagPerTopic = MultiGauge.builder("datavortex.kafka.consumer.lag")
                    .description("Μηνύματα που δεν έχει διαβάσει ακόμα ο consumer, ανά topic")
                    .baseUnit("records")
                    .register(registry);
            registry.gauge("datavortex.kafka.consumer.lag.total", totals,
                    m -> m.values().stream().mapToLong(Long::longValue).sum());
        }

        @Scheduled(initialDelay = 10_000, fixedDelayString = "${datavortex.lag-refresh-ms:15000}")
        void refresh() {
            try {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                                .get(10, TimeUnit.SECONDS);
                if (committed.isEmpty()) {
                    return;
                }
                Map<TopicPartition, OffsetSpec> request = committed.keySet().stream()
                        .filter(tp -> consumerPattern.matcher(tp.topic()).matches())
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
                if (request.isEmpty()) {
                    return;
                }
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                        admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);

                Map<String, Long> perTopic = new ConcurrentHashMap<>();
                committed.forEach((tp, offset) -> {
                    ListOffsetsResult.ListOffsetsResultInfo end = ends.get(tp);
                    if (!consumerPattern.matcher(tp.topic()).matches()) {
                        return;
                    }
                    if (end == null || offset == null) {
                        return;
                    }
                    long lag = Math.max(0, end.offset() - offset.offset());
                    perTopic.merge(tp.topic(), lag, Long::sum);
                });

                totals.clear();
                totals.putAll(perTopic);
                List<MultiGauge.Row<?>> rows = new ArrayList<>(perTopic.size());
                perTopic.forEach((topic, lag) -> rows.add(MultiGauge.Row.of(Tags.of("topic", topic), lag)));
                lagPerTopic.register(rows, true);

            } catch (Exception e) {
                // Δεν είναι κρίσιμο: αν ο broker δεν απαντά, μένουν οι προηγούμενες τιμές.
                log.debug("Δεν υπολογίστηκε το consumer lag: {}", e.getMessage());
            }
        }

        @Override
        public void destroy() {
            admin.close(java.time.Duration.ofSeconds(2));
        }
    }
}
