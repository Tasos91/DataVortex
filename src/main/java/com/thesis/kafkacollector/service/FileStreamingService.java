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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Το αποτέλεσμα μιας εισαγωγής. Το {@code accepted} είναι όσα επιβεβαίωσε το Kafka
     * (μετά από flush), όχι όσα μπήκαν στην ουρά του producer.
     *
     * @param read     πόσες εγγραφές διαβάστηκαν από την είσοδο
     * @param accepted πόσες επιβεβαιώθηκαν από το Kafka
     * @param skipped  πόσες ήταν άκυρες και προσπεράστηκαν
     */
    public record IngestResult(int read, int accepted, int skipped) {
        public int failed() {
            return read - skipped - accepted;
        }
    }

    public IngestResult processCsvStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        String topic = resolveTopic(request);
        AtomicInteger failed = new AtomicInteger();
        int read = 0;
        int skipped = 0;

        try (BufferedReader br = Files.newBufferedReader(Paths.get(request.getFilePath()), StandardCharsets.UTF_8)) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                return new IngestResult(0, 0, 0);
            }

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

                    send(topic, request, objectMapper.valueToTree(rawJsonMap), mappings, failed);
                    read++;

                } catch (Exception e) {
                    skipped++;
                    log.warn("Σφάλμα κατά την επεξεργασία της γραμμής CSV: {}. Αιτία: {}", line, e.getMessage());
                }
            }
        }
        return finish(topic, "CSV", read + skipped, read, skipped, failed);
    }

    /**
     * Δέχεται το JSON payload απευθείας μέσα στο request και το στέλνει στο Kafka ως Protobuf,
     * χαρτογραφώντας τα πεδία που όρισε ο χρήστης στο fieldMappings.
     * Αν το payload είναι πίνακας, κάθε στοιχείο γίνεται ξεχωριστό μήνυμα.
     * <p>
     * ΠΡΟΣΟΧΗ: εδώ ολόκληρο το payload βρίσκεται στη μνήμη (το έχει δέσει το Jackson από το
     * σώμα του request). Για μεγάλο όγκο χρησιμοποίησε το {@link #processNdjson}.
     */
    public IngestResult processPayload(IngestionRequest request) throws Exception {
        JsonNode root = objectMapper.valueToTree(request.getPayload());
        Map<String, String> mappings = request.getFieldMappings();
        String topic = resolveTopic(request);
        AtomicInteger failed = new AtomicInteger();
        int read = 0;

        if (root.isArray()) {
            for (JsonNode element : root) {
                send(topic, request, element, mappings, failed);
                read++;
            }
        } else {
            send(topic, request, root, mappings, failed);
            read++;
        }
        return finish(topic, "Payload", read, read, 0, failed);
    }

    /**
     * Streaming εισαγωγή: NDJSON, δηλαδή ένα JSON αντικείμενο ανά γραμμή.
     * <p>
     * Η <b>πρώτη</b> γραμμή είναι οι ρυθμίσεις (sourceId, topic, keyField, fieldMappings) και
     * οι επόμενες είναι οι εγγραφές. Σε κάθε στιγμή στη μνήμη βρίσκεται μία γραμμή, άρα το
     * μέγεθος της εισόδου δεν έχει όριο — σε αντίθεση με το {@link #processPayload}, όπου
     * ολόκληρο το σώμα του request δένεται στη μνήμη (και σε αρκετά αντίγραφα).
     * <p>
     * Μια χαλασμένη γραμμή προσπερνιέται και μετριέται· δεν ρίχνει όλη την εισαγωγή.
     */
    public IngestResult processNdjson(InputStream body) throws Exception {
        IngestionRequest config = null;
        String topic = null;
        Map<String, String> mappings = null;
        AtomicInteger failed = new AtomicInteger();
        int read = 0;
        int skipped = 0;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                if (config == null) {
                    config = objectMapper.readValue(line, IngestionRequest.class);
                    topic = resolveTopic(config);
                    mappings = config.getFieldMappings();
                    log.info("NDJSON: ξεκινά εισαγωγή στο topic {} (sourceId {})", topic, config.getSourceId());
                    continue;
                }
                try {
                    send(topic, config, objectMapper.readTree(line), mappings, failed);
                    read++;
                } catch (Exception e) {
                    skipped++;
                    log.warn("Γραμμή NDJSON που προσπεράστηκε: {}", e.getMessage());
                }
            }
        }
        if (config == null) {
            throw new IllegalArgumentException("Το σώμα είναι κενό. Η πρώτη γραμμή πρέπει να έχει τις "
                    + "ρυθμίσεις (sourceId, topic, keyField, fieldMappings) και οι επόμενες τις εγγραφές.");
        }
        return finish(topic, "NDJSON", read + skipped, read, skipped, failed);
    }

    public IngestResult processJsonStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        String topic = resolveTopic(request);
        AtomicInteger failed = new AtomicInteger();
        log.info("Ξεκινάει η επεξεργασία JSON αρχείου: {}", request.getFilePath());
        log.info("Στόχος Kafka Topic: {}", topic);

        com.fasterxml.jackson.core.JsonFactory factory = objectMapper.getFactory();
        int read = 0;
        int skipped = 0;

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
                        send(topic, request, objectMapper.readTree(parser), mappings, failed);
                        read++;

                    } catch (Exception e) {
                        skipped++;
                        log.warn("Σφάλμα κατά την επεξεργασία ενός JSON object. Αιτία: {}", e.getMessage());
                    }
                }
                token = parser.nextToken();
            }
        }
        return finish(topic, "JSON αρχείο", read + skipped, read, skipped, failed);
    }

    /**
     * Περιμένει να επιβεβαιώσει το Kafka ό,τι στάλθηκε, πριν απαντήσουμε στον client.
     * Χωρίς αυτό η απάντηση έλεγε "στάλθηκαν N" ενώ τα μηνύματα ήταν ακόμα στο buffer του
     * producer: αν έπεφτε το Kafka, ο client είχε 200 και τα δεδομένα δεν υπήρχαν πουθενά.
     */
    private IngestResult finish(String topic, String what, int total, int read, int skipped, AtomicInteger failed) {
        kafkaTemplate.flush();
        int accepted = read - failed.get();
        IngestResult result = new IngestResult(total, accepted, skipped);
        if (result.failed() > 0 || skipped > 0) {
            log.warn("{}: {} εγγραφές, {} στο Kafka, {} απέτυχαν, {} άκυρες (topic {})",
                    what, total, accepted, result.failed(), skipped, topic);
        } else {
            log.info("{}: {} μηνύματα στο topic {}", what, accepted, topic);
        }
        return result;
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
    private void send(String topic, IngestionRequest request, JsonNode node, Map<String, String> mappings,
                      AtomicInteger failed) throws Exception {
        IngestedRecordProto.IngestedRecord record = RecordMapper.map(
                request.getSourceId(), objectMapper.writeValueAsString(node), node, mappings);
        String key = RecordMapper.keyOf(node, request.getKeyField());

        kafkaTemplate.send(topic, key, record).whenComplete((result, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                log.error("Αποτυχία αποστολής στο Kafka (topic {}, sourceId {}): {}",
                        topic, request.getSourceId(), ex.getMessage());
            }
        });
    }
}
