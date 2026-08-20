package com.thesis.kafkacollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
// Αυτό λέει στο Spring: "Μην ψάξεις καθόλου για βάση δεδομένων προς το παρόν!"
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class KafkaCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaCollectorApplication.class, args);
	}

}
