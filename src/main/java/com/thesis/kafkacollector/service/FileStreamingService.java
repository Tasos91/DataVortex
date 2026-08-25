package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.entity.IngestedRecord;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
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

    // ΑΛΛΑΓΗ: Το KafkaTemplate πλέον στέλνει byte[] (binary) αντί για String
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Όρισε το όνομα του topic (μπορείς να το βάλεις και στο application.properties)
    @Value("${kafka.topic.ingestion:ais-data}")
    private String topic;

    public FileStreamingService(KafkaTemplate<String, byte[]> kafkaTemplate) {
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
                // ΕΣΩΤΕΡΙΚΟ TRY-CATCH: Για να μη σταματήσει το loop αν μια γραμμή είναι "χαλασμένη"
                try {
                    String[] values = line.split(",");
                    Map<String, Object> rawJsonMap = new HashMap<>();

                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        rawJsonMap.put(headers[i].trim(), values[i].trim());
                    }

                    Double lat = extractDouble(rawJsonMap, mappings.get("lat"));
                    Double lon = extractDouble(rawJsonMap, mappings.get("lon"));
                    String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                    // ΑΛΛΑΓΗ: Δημιουργία του αντικειμένου με τον Builder του Protobuf
                    IngestedRecordProto.IngestedRecord record = IngestedRecordProto.IngestedRecord.newBuilder()
                            .setSourceId(request.getSourceId() != null ? request.getSourceId() : "")
                            .setPayloadJson(payloadJson != null ? payloadJson : "")
                            .setLat(lat != null ? lat : 0.0)
                            .setLon(lon != null ? lon : 0.0)
                            .build();

                    // ΑΛΛΑΓΗ: Μετατροπή ολόκληρου του record σε byte[] (binary) για να σταλεί στο Kafka
                    byte[] messageToSend = record.toByteArray();

                    // Αποστολή στο Kafka Topic (βάζουμε το SourceId ως κλειδί του μηνύματος)
                    kafkaTemplate.send(topic, request.getSourceId(), messageToSend).whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Αποτυχία αποστολής στο Kafka για το sourceId: {}", request.getSourceId(), ex);
                        }
                    });

                } catch (Exception e) {
                    log.warn("Σφάλμα κατά την επεξεργασία της γραμμής CSV: {}. Αιτία: {}", line, e.getMessage());
                }
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
                    // ΕΣΩΤΕΡΙΚΟ TRY-CATCH
                    try {
                        Map<String, Object> rawJsonMap = objectMapper.readValue(parser, Map.class);

                        Double lat = extractDouble(rawJsonMap, mappings.get("lat"));
                        Double lon = extractDouble(rawJsonMap, mappings.get("lon"));
                        String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                        // ΑΛΛΑΓΗ: Δημιουργία του αντικειμένου με τον Builder του Protobuf
                        IngestedRecordProto.IngestedRecord record = IngestedRecordProto.IngestedRecord.newBuilder()
                                .setSourceId(request.getSourceId() != null ? request.getSourceId() : "")
                                .setPayloadJson(payloadJson != null ? payloadJson : "")
                                .setLat(lat != null ? lat : 0.0)
                                .setLon(lon != null ? lon : 0.0)
                                .build();

                        // ΑΛΛΑΓΗ: Μετατροπή σε byte[]
                        byte[] messageToSend = record.toByteArray();

                        kafkaTemplate.send(topic, request.getSourceId(), messageToSend).whenComplete((result, ex) -> {
                            if (ex != null) {
                                log.error("Αποτυχία αποστολής στο Kafka για το sourceId: {}", request.getSourceId(), ex);
                            }
                        });
                        recordCount++;

                    } catch (Exception e) {
                        log.warn("Σφάλμα κατά την επεξεργασία ενός JSON object. Αιτία: {}", e.getMessage());
                    }
                }
                token = parser.nextToken();
            }
        }
        log.info("Ολοκληρώθηκε η αποστολή. Στάλθηκαν συνολικά {} μηνύματα στο Kafka!", recordCount);
    }

}