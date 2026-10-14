package com.thesis.kafkacollector.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Το ingestion-service είναι υγιές μόνο αν ο Kafka broker απαντά.
 * <p>
 * Είναι η μοναδική του έξοδος: χωρίς broker δεν μπορεί να δεχτεί τίποτα, αλλά το
 * {@code /actuator/health} θα έλεγε UP γιατί η διεργασία ζει. Ο έλεγχος είναι φθηνός
 * (describeCluster) και με ρητό χρονικό όριο, ώστε ένας νεκρός broker να μη κρεμάει το health.
 */
@Component("kafka")
public class KafkaHealth implements HealthIndicator, DisposableBean {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final AdminClient admin;

    public KafkaHealth(KafkaAdmin kafkaAdmin) {
        this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Override
    public Health health() {
        try {
            DescribeClusterResult cluster = admin.describeCluster();
            int brokers = cluster.nodes().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).size();
            return Health.up()
                    .withDetail("clusterId", cluster.clusterId().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                    .withDetail("brokers", brokers)
                    .build();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("error", String.valueOf(e.getMessage())).build();
        }
    }

    @Override
    public void destroy() {
        admin.close(Duration.ofSeconds(2));
    }
}
