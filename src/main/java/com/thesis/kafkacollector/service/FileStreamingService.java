package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.dto.IngestionRequest;
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
    private final KafkaTopicService topicService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Το topic που χρησιμοποιείται όταν το request δεν ορίζει δικό του
    @Value("${spring.kafka.topic.ingestion:ais-data}")
    private String defaultTopic;

    public FileStreamingService(KafkaTemplate<String, IngestedRecordProto.IngestedRecord> kafkaTemplate,
                                KafkaTopicService topicService) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicService = topicService;
    }

    public void processCsvStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        String topic = resolveTopic(request);
        int sent = 0;

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

                    send(topic, request, objectMapper.valueToTree(rawJsonMap), mappings);
                    sent++;

                } catch (Exception e) {
                    log.warn("Σφάλμα κατά την επεξεργασία της γραμμής CSV: {}. Αιτία: {}", line, e.getMessage());
                }
            }
        }
        log.info("CSV: στάλθηκαν {} μηνύματα στο topic {}", sent, topic);
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
        String topic = resolveTopic(request);
        int sent = 0;

        if (root.isArray()) {
            for (JsonNode element : root) {
                send(topic, request, element, mappings);
                sent++;
            }
        } else {
            send(topic, request, root, mappings);
            sent++;
        }
        log.info("Payload: στάλθηκαν {} μηνύματα στο topic {}", sent, topic);
        return sent;
    }

    public void processJsonStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        String topic = resolveTopic(request);
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
                        send(topic, request, objectMapper.readTree(parser), mappings);
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

    /**
     * Το topic του request, ή το default. Ελέγχεται ότι υπάρχει πριν στείλουμε οτιδήποτε,
     * ώστε ένα λάθος όνομα να γίνεται αμέσως 400 και να μη κολλάει ο producer.
     */
    private String resolveTopic(IngestionRequest request) {
        String topic = (request.getTopic() != null && !request.getTopic().isBlank())
                ? request.getTopic().trim()
                : defaultTopic;
        topicService.ensureUsable(topic, request.isCreateTopicIfMissing(), request.getPartitions());
        return topic;
    }

    /**
     * Χτίζει το Protobuf record και το στέλνει. Το κλειδί βγαίνει από το keyField του request:
     * ίδιο κλειδί σημαίνει ίδιο partition, άρα εγγυημένη σειρά για την ίδια οντότητα.
     * Χωρίς keyField το κλειδί είναι null και τα μηνύματα μοιράζονται σε όλα τα partitions.
     */
    private void send(String topic, IngestionRequest request, JsonNode node, Map<String, String> mappings)
            throws Exception {
        IngestedRecordProto.IngestedRecord record = RecordMapper.map(
                request.getSourceId(), objectMapper.writeValueAsString(node), node, mappings);
        String key = RecordMapper.keyOf(node, request.getKeyField());

        kafkaTemplate.send(topic, key, record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Αποτυχία αποστολής στο Kafka (topic {}, sourceId {})", topic, request.getSourceId(), ex);
            }
        });
    }
}
