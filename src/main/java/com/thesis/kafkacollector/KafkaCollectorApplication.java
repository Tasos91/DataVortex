package com.thesis.kafkacollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
// Η βάση είναι ΕΝΕΡΓΗ: εκεί γράφει ο consumer και εκεί τρέχει το Flyway στο startup.
// Γράφουμε με JdbcTemplate σε batches, χωρίς JPA/Hibernate (δες CLAUDE.md).
@SpringBootApplication
public class KafkaCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaCollectorApplication.class, args);
	}

}
