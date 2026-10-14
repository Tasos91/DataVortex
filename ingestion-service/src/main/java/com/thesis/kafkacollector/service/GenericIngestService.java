package com.thesis.kafkacollector.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.format.FieldSpec;
import com.thesis.kafkacollector.format.IngestSpec;
import com.thesis.kafkacollector.format.JsonRecords;
import com.thesis.kafkacollector.format.RecordPath;
import com.thesis.kafkacollector.format.XmlRecords;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Μία εισαγωγή, οποιαδήποτε μορφή.
 * <p>
 * Το {@link IngestSpec} λέει τι θέλει ο χρήστης· εδώ εκτελείται. Οι μορφές διαφέρουν μόνο στο
 * <b>πώς παράγονται οι εγγραφές</b>:
 * <ul>
 *   <li><b>CSV</b> → περνά στη δική του γρήγορη διαδρομή ({@link CsvIngestService}), όπου τα πεδία
 *       είναι θέσεις σε πίνακα και δεν χτίζεται ποτέ δέντρο·</li>
 *   <li><b>NDJSON</b> → μία γραμμή, μία εγγραφή·</li>
 *   <li><b>JSON</b> → πλοήγηση σε ροή μέχρι τις εγγραφές ({@link JsonRecords})·</li>
 *   <li><b>XML</b> → StAX, ένα στοιχείο τη φορά ({@link XmlRecords}).</li>
 * </ul>
 * Από εκεί και πέρα η διαδρομή είναι <b>κοινή</b>: κάθε εγγραφή είναι ένα δέντρο, τα πεδία
 * διαβάζονται με τις μεταγλωττισμένες διαδρομές τους, και φεύγει ένα μήνυμα.
 */
@Service
public class GenericIngestService {

    private static final Logger log = LoggerFactory.getLogger(GenericIngestService.class);
    /** Πόσες χαλασμένες εγγραφές καταγράφονται πριν σωπάσουμε. */
    private static final int MAX_LOGGED = 5;

    private final RecordSender sender;
    private final KafkaTopicService topicService;
    private final IngestMeters meters;
    private final CsvIngestService csvService;
    private final ObjectMapper mapper = new ObjectMapper();

    public GenericIngestService(RecordSender sender, KafkaTopicService topicService,
                                IngestMeters meters, CsvIngestService csvService) {
        this.sender = sender;
        this.topicService = topicService;
        this.meters = meters;
        this.csvService = csvService;
    }

    /**
     * @param in     η ροή των δεδομένων· κλείνεται από εδώ
     * @param spec   τι διάλεξε ο χρήστης
     * @param format η μορφή, ήδη αποφασισμένη (δηλωμένη ή ανιχνευμένη)
     */
    public IngestResult ingest(Reader in, IngestSpec spec, IngestSpec.Format format) throws Exception {
        if (format == IngestSpec.Format.CSV) {
            return csvService.ingest(in, toCsvPlan(spec));
        }
        spec.validate(null); // για τις δομές δεν ξέρουμε από πριν τι υπάρχει· ελέγχουμε ό,τι μπορούμε
        String sourceId = spec.sourceIdOr("data");
        String topic = topicService.resolve(spec.topic(), spec.createTopicIfMissing(), spec.partitions());
        RecordSender.Session session = sender.open(topic);
        Fields fields = Fields.compile(spec);

        Counters counters = new Counters();
        try (Reader reader = in) {
            switch (format) {
                case NDJSON -> readNdjson(reader, spec, fields, sourceId, session, counters);
                case JSON -> readJson(reader, spec, fields, sourceId, session, counters);
                case XML -> readXml(reader, spec, fields, sourceId, session, counters);
                default -> throw new IllegalArgumentException("Μη υποστηριζόμενη μορφή: " + format);
            }
        }
        if (counters.noTime > 0) {
            log.warn("{} από {} εγγραφές δεν είχαν διαβάσιμο χρόνο στο '{}' και πήραν την ώρα της "
                    + "εισαγωγής.", counters.noTime, counters.read, spec.timeField());
        }
        return meters.finish(format.name().toLowerCase(java.util.Locale.ROOT),
                counters.read + counters.skipped, counters.read, counters.skipped, session);
    }

    // ----------------------------------------------------------------- ανά μορφή

    private void readNdjson(Reader in, IngestSpec spec, Fields fields, String sourceId,
                            RecordSender.Session session, Counters counters) throws Exception {
        RecordPath records = RecordPath.of(spec.recordPath());
        try (BufferedReader br = new BufferedReader(in, 1 << 16)) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                try {
                    JsonNode root = mapper.readTree(line);
                    records.forEachWithScope(root, (record, scope) ->
                            emit(record, scope, spec, fields, sourceId, session, counters));
                } catch (Exception e) {
                    counters.skip(e, log);
                }
            }
        }
    }

    private void readJson(Reader in, IngestSpec spec, Fields fields, String sourceId,
                          RecordSender.Session session, Counters counters) throws Exception {
        String path = spec.recordPath();
        if (JsonRecords.isStreamable(path) && !fields.needsAncestors()) {
            // Η συνηθισμένη περίπτωση: πλοήγηση σε ροή, μία εγγραφή στη μνήμη.
            JsonRecords.forEach(in, path, mapper,
                    record -> emit(record, null, spec, fields, sourceId, session, counters));
            return;
        }
        // Διαδρομή που ξεδιπλώνεται σε πολλά επίπεδα: χρειάζεται ολόκληρο το δέντρο.
        log.info("Η διαδρομή '{}' ξεδιπλώνεται σε πολλά επίπεδα· το έγγραφο φορτώνεται ολόκληρο. "
                + "Για πολύ μεγάλα αρχεία προτίμησε NDJSON ή απλούστερο recordPath.", path);
        JsonNode root = mapper.readTree(in);
        RecordPath.of(path).forEachWithScope(root, (record, scope) ->
                emit(record, scope, spec, fields, sourceId, session, counters));
    }

    private void readXml(Reader in, IngestSpec spec, Fields fields, String sourceId,
                         RecordSender.Session session, Counters counters) throws Exception {
        // Για XML το recordPath είναι το όνομα του στοιχείου που αποτελεί εγγραφή.
        String element = spec.recordPath() == null ? null : spec.recordPath().strip().replace("$.", "");
        XmlRecords.forEachWithScope(in, element,
                (record, scope) -> emit(record, scope, spec, fields, sourceId, session, counters));
    }

    // ----------------------------------------------------------------- κοινή διαδρομή

    /**
     * Μία εγγραφή: πεδία, κλειδί, χρόνος, αποστολή.
     *
     * @param scope οι πρόγονοι της εγγραφής, για τα πεδία με {@code $^} και {@code $$}·
     *              {@code null} όταν η ανάγνωση είναι σε ροή και δεν υπάρχουν πρόγονοι
     */
    private void emit(JsonNode record, RecordPath.Scope scope, IngestSpec spec, Fields fields,
                      String sourceId, RecordSender.Session session, Counters counters) {
        try {
            IngestedRecord.Builder builder = IngestedRecord.newBuilder().setSourceId(sourceId);
            for (FieldSpec.Compiled field : fields.values) {
                Value value = field.read(record, scope);
                if (value != null) {
                    builder.putFields(field.name(), value);
                }
            }
            if (spec.keepRaw()) {
                builder.setPayloadJson(record.toString());
            }
            Long eventTime = fields.time == null ? null : fields.time.readTime(record, scope);
            String key = fields.key == null ? null : fields.key.readKey(record, scope);

            session.send(eventTime, key, builder.build());
            counters.read++;
            if (fields.time != null && eventTime == null) {
                counters.noTime++;
            }
        } catch (RuntimeException e) {
            counters.skip(e, log);
        }
    }

    /** Τα πεδία μεταγλωττισμένα μία φορά: μέσα στον βρόχο δεν αναλύεται καμία διαδρομή. */
    private record Fields(List<FieldSpec.Compiled> values, FieldSpec.Compiled time, FieldSpec.Compiled key) {

        /**
         * Χρειάζεται κάποιο πεδίο πρόσβαση σε γονέα ή στη ρίζα; Τότε η ανάγνωση σε ροή δεν αρκεί:
         * ο parser προχωρά μπροστά και δεν κρατά ό,τι άφησε πίσω του.
         */
        boolean needsAncestors() {
            if (needs(time) || needs(key)) {
                return true;
            }
            for (FieldSpec.Compiled field : values) {
                if (needs(field)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean needs(FieldSpec.Compiled field) {
            return field != null && (field.fromRoot() || field.parentLevels() > 0);
        }

        static Fields compile(IngestSpec spec) {
            List<FieldSpec.Compiled> values = new ArrayList<>(spec.fields().size());
            for (FieldSpec field : spec.fields()) {
                values.add(field.compile());
            }
            return new Fields(values,
                    compileOne(spec.timeField(), spec.timeFormat(), "__time"),
                    compileOne(spec.keyField(), null, "__key"));
        }

        private static FieldSpec.Compiled compileOne(String path, String format, String name) {
            if (path == null || path.isBlank()) {
                return null;
            }
            return new FieldSpec(name, path, null, null, null, format, null).compile();
        }
    }

    /** Μετρητές μιας εισαγωγής σε εξέλιξη. */
    private static final class Counters {
        int read;
        int skipped;
        int noTime;

        void skip(Exception e, Logger log) {
            skipped++;
            if (skipped <= MAX_LOGGED) {
                log.warn("Εγγραφή που προσπεράστηκε: {}", e.toString());
            }
        }
    }

    /**
     * Το γενικό σχέδιο σε σχέδιο CSV. Για το CSV υπάρχει ξεχωριστή διαδρομή επίτηδες: εκεί τα
     * πεδία είναι <b>θέσεις σε πίνακα</b> και δεν χτίζεται ποτέ δέντρο JSON — κάτι που σε ένα
     * αρχείο 200.000 γραμμών είναι η διαφορά ανάμεσα σε 743 ms και πολλαπλάσιο χρόνο.
     */
    static CsvPlan toCsvPlan(IngestSpec spec) {
        List<CsvPlan.Column> columns = new ArrayList<>();
        for (FieldSpec field : spec.fields()) {
            columns.add(new CsvPlan.Column(IngestSpec.stripDollar(field.path()), field.name(),
                    field.type()));
        }
        IngestSpec.Options options = spec.opts();
        return new CsvPlan(spec.sourceId(), spec.topic(), spec.createTopicIfMissing(), spec.partitions(),
                options.delimiter(), options.decimal(), options.hasHeader(),
                IngestSpec.stripDollar(spec.timeField()), spec.timeFormat(),
                IngestSpec.stripDollar(spec.keyField()), columns, spec.keepRaw());
    }
}
