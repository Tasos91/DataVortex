package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.entity.IngestedRecord;
import com.thesis.kafkacollector.mapping.RecordMapper;
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

    // Το KafkaProtobufSerializer γράφει το schema στο Schema Registry και στέλνει το record με το schema id
    private final KafkaTemplate<String, IngestedRecordProto.IngestedRecord> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Όρισε το όνομα του topic (μπορείς να το βάλεις και στο application.properties)
    @Value("${spring.kafka.topic.ingestion:ais-data}")
    private String topic;

    public FileStreamingService(KafkaTemplate<String, IngestedRecordProto.IngestedRecord> kafkaTemplate) {
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

                    String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                    IngestedRecordProto.IngestedRecord record = RecordMapper.map(
                            request.getSourceId(), payloadJson, objectMapper.valueToTree(rawJsonMap), mappings);

                    // Αποστολή στο Kafka Topic (βάζουμε το SourceId ως κλειδί του μηνύματος)
                    kafkaTemplate.send(topic, request.getSourceId(), record).whenComplete((result, ex) -> {
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

    /**
     * Δέχεται το JSON payload απευθείας μέσα στο request και το στέλνει στο Kafka ως Protobuf,
     * χαρτογραφώντας τα πεδία που όρισε ο χρήστης στο fieldMappings.
     * Αν το payload είναι πίνακας, κάθε στοιχείο γίνεται ξεχωριστό μήνυμα.
     *
     * @return πόσα μηνύματα στάλθηκαν
     */
    public int processPayload(IngestionRequest request) throws Exception {
        JsonNode root = objectMapper.valueToTree(request.getPayload());
        Map<String, String> mappings = request.getFieldMappings();
        int sent = 0;

        if (root.isArray()) {
            for (JsonNode element : root) {
                sendRecord(request, element, mappings);
                sent++;
            }
        } else {
            sendRecord(request, root, mappings);
            sent++;
        }
        log.info("Payload: στάλθηκαν {} μηνύματα στο topic {}", sent, topic);
        return sent;
    }

    private void sendRecord(IngestionRequest request, JsonNode node, Map<String, String> mappings) throws Exception {
        IngestedRecordProto.IngestedRecord record = RecordMapper.map(
                request.getSourceId(), objectMapper.writeValueAsString(node), node, mappings);
        kafkaTemplate.send(topic, request.getSourceId(), record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Αποτυχία αποστολής στο Kafka για το sourceId: {}", request.getSourceId(), ex);
            }
        });
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

                        String payloadJson = objectMapper.writeValueAsString(rawJsonMap);

                        IngestedRecordProto.IngestedRecord record = RecordMapper.map(
                                request.getSourceId(), payloadJson, objectMapper.valueToTree(rawJsonMap), mappings);

                        kafkaTemplate.send(topic, request.getSourceId(), record).whenComplete((result, ex) -> {
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