//package com.thesis.kafkacollector.service;
//
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Service;
//
//@Slf4j
//@Service
//public class KafkaConsumerService {
//
//    private final DashboardMetricsService metricsService;
//
//    public KafkaConsumerService(DashboardMetricsService metricsService) {
//        this.metricsService = metricsService;
//    }
//
//    @KafkaListener(
//            topics = "${kafka.topic.ingestion:ais-data}",
//            groupId = "thesis-group-final",
//            properties = {"auto.offset.reset=earliest"}
//    )
//    public void listen(String message) {
//        log.info("🔥 CONSUMER RECEIVED: {}", message);
//        metricsService.recordMessage(message);
//    }
//}