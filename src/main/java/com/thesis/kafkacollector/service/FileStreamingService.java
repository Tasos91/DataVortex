package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.entity.IngestedRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class FileStreamingService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Όρισε το όνομα του topic (μπορείς να το βάλεις και στο application.properties)
    @Value("${kafka.topic.ingestion:ais-data}")
    private String topic;

    public FileStreamingService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void processCsvStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();

        try (BufferedReader br = Files.newBufferedReader(Paths.get(request.getFilePath()), StandardCharsets.UTF_8)) {
            String headerLine = br.readLine();
            if (headerLine == null) return;

            String[] headers = headerLine.split(",");
            String line;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, Object> rawJsonMap = new HashMap<>();

                for (int i = 0; i < headers.length && i < values.length; i++) {
                    rawJsonMap.put(headers[i].trim(), values[i].trim());
                }

                Double lat = extractDouble(rawJsonMap, mappings.get("lat"));
                Double lon = extractDouble(rawJsonMap, mappings.get("lon"));
                String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                // Δημιουργία του αντικειμένου
                IngestedRecord record = new IngestedRecord(
                        request.getSourceId(),
                        payloadJson,
                        lat,
                        lon
                );
                // Μετατροπή ολόκληρου του record σε JSON για να σταλεί στο Kafka
                String messageToSend = objectMapper.writeValueAsString(record);

                // Αποστολή στο Kafka Topic
                kafkaTemplate.send(topic, messageToSend);
            }
        }
    }

    private Double extractDouble(Map<String, Object> map, String key) {
        if (key == null || !map.containsKey(key)) return null;
        try {
            return Double.parseDouble(map.get(key).toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
    public void processJsonStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        log.info("Ξεκινάει η επεξεργασία JSON αρχείου: {}", request.getFilePath());
        log.info("Στόχος Kafka Topic: {}", topic);

        com.fasterxml.jackson.core.JsonFactory factory = objectMapper.getFactory();
        int recordCount = 0;

        try (BufferedReader br = Files.newBufferedReader(Paths.get(request.getFilePath()), StandardCharsets.UTF_8);
             com.fasterxml.jackson.core.JsonParser parser = factory.createParser(br)) {

            com.fasterxml.jackson.core.JsonToken token = parser.nextToken();
            log.info("Πρώτο JSON token: {}", token);

            if (token == com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
                token = parser.nextToken();
            }

            while (token != null && token != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                if (token == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                    Map<String, Object> rawJsonMap = objectMapper.readValue(parser, Map.class);

                    Double lat = extractDouble(rawJsonMap, mappings.get("lat"));
                    Double lon = extractDouble(rawJsonMap, mappings.get("lon"));
                    String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                    IngestedRecord record = new IngestedRecord(
                            request.getSourceId(),
                            payloadJson,
                            lat,
                            lon
                    );

                    String messageToSend = objectMapper.writeValueAsString(record);
                    kafkaTemplate.send(topic, messageToSend);
                    recordCount++;
                }
                token = parser.nextToken();
            }
        }
        log.info("Ολοκληρώθηκε η αποστολή. Στάλθηκαν συνολικά {} μηνύματα στο Kafka!", recordCount);
    }

}