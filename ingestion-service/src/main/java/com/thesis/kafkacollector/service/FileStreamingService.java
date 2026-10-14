package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.csv.CsvInspector;
import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.dto.IngestionRequest;
import com.thesis.kafkacollector.mapping.RecordMapper;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class FileStreamingService {

    private static final Logger log = LoggerFactory.getLogger(FileStreamingService.class);

    // Το KafkaProtobufSerializer γράφει το schema στο Schema Registry και στέλνει το record με το schema id
    private final KafkaTemplate<String, IngestedRecordProto.IngestedRecord> kafkaTemplate;
    private final KafkaTopicService topicService;
    private final CsvIngestService csvService;
    private final IngestMeters meters;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FileStreamingService(KafkaTemplate<String, IngestedRecordProto.IngestedRecord> kafkaTemplate,
                                KafkaTopicService topicService, CsvIngestService csvService,
                                IngestMeters meters) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicService = topicService;
        this.csvService = csvService;
        this.meters = meters;
    }

    /**
     * CSV από αρχείο, με την παλιά μορφή request ({@code fileType: "CSV"} + {@code fieldMappings}).
     * <p>
     * Μένει για συμβατότητα, αλλά <b>δεν</b> διαβάζει πια μόνο του: μεταφράζει το request σε
     * {@link CsvPlan} και δουλεύει το {@link CsvIngestService}. Έτσι κερδίζει και αυτή η διαδρομή
     * τον σωστό parser (εισαγωγικά, CRLF, BOM) και τους <b>τύπους</b>: πριν, κάθε στήλη πήγαινε ως
     * κείμενο και κανένα CSV δεν παρήγαγε ποτέ μετρική.
     * <p>
     * Οι τύποι δεν δηλώνονται εδώ, άρα μαντεύονται από τα δεδομένα — γι' αυτό το
     * {@code /api/v1/csv/inspect} + {@code /ingest} είναι ο σωστός δρόμος: εκεί τους λέει ο χρήστης.
     */
    public IngestResult processCsvStream(IngestionRequest request) throws Exception {
        java.nio.file.Path file = Paths.get(request.getFilePath());
        String sample;
        try (var in = Files.newInputStream(file)) {
            sample = new String(in.readNBytes(256 * 1024), StandardCharsets.UTF_8);
        }
        CsvInspector.Profile profile = CsvInspector.inspect(sample, file.getFileName().toString());

        // Αν ο χρήστης έδωσε fieldMappings, σέβονται απόλυτα: κρατάμε μόνο αυτές τις στήλες, με τα
        // ονόματα που ζήτησε. Ο τύπος κάθε στήλης έρχεται από την επιθεώρηση του αρχείου.
        List<CsvPlan.Column> columns = new java.util.ArrayList<>();
        Map<String, String> mappings = request.getFieldMappings();
        if (mappings == null || mappings.isEmpty()) {
            columns.addAll(profile.suggestion().columns());
        } else {
            for (Map.Entry<String, String> e : mappings.entrySet()) {
                String column = stripPath(e.getValue());
                columns.add(new CsvPlan.Column(column, e.getKey(), typeOf(profile, column)));
            }
        }
        CsvPlan plan = new CsvPlan(request.getSourceId(), request.getTopic(),
                request.isCreateTopicIfMissing(), request.getPartitions(),
                profile.delimiter(), profile.suggestion().decimal(), profile.hasHeader(),
                stripPath(request.getTimeField()), null, stripPath(request.getKeyField()),
                columns, true);

        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return csvService.ingest(reader, plan);
        }
    }

    /** Το {@code "$.col"} ή {@code "col"} του παλιού request είναι όνομα στήλης στο CSV. */
    private static String stripPath(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String p = path.strip();
        return p.startsWith("$.") ? p.substring(2) : p;
    }

    private static String typeOf(CsvInspector.Profile profile, String column) {
        for (CsvInspector.Column c : profile.columns()) {
            if (c.name().equals(column)) {
                return c.type();
            }
        }
        return "text";
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
        String topic = topicService.resolve(request.getTopic(), request.isCreateTopicIfMissing(), request.getPartitions());
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
        return meters.finish(topic, "payload", read, read, 0, failed);
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
                    topic = topicService.resolve(config.getTopic(), config.isCreateTopicIfMissing(), config.getPartitions());
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
        return meters.finish(topic, "ndjson", read + skipped, read, skipped, failed);
    }

    public IngestResult processJsonStream(IngestionRequest request) throws Exception {
        Map<String, String> mappings = request.getFieldMappings();
        String topic = topicService.resolve(request.getTopic(), request.isCreateTopicIfMissing(), request.getPartitions());
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
        return meters.finish(topic, "json-file", read + skipped, read, skipped, failed);
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
        Long eventTime = RecordMapper.eventTimeOf(node, request.getTimeField());

        // Ο χρόνος του μηνύματος στο Kafka γίνεται event_time στη βάση. Με timeField μπορούμε
        // να φορτώσουμε ιστορικά δεδομένα με τον σωστό τους χρόνο (backfill).
        var producerRecord = new org.apache.kafka.clients.producer.ProducerRecord<>(
                topic, null, eventTime, key, record);
        kafkaTemplate.send(producerRecord).whenComplete((result, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                log.error("Αποτυχία αποστολής στο Kafka (topic {}, sourceId {}): {}",
                        topic, request.getSourceId(), ex.getMessage());
            }
        });
    }
}
