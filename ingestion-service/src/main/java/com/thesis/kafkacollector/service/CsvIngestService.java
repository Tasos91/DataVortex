package com.thesis.kafkacollector.service;

import com.google.protobuf.Value;
import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.csv.CsvReader;
import com.thesis.kafkacollector.csv.CsvValues;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.Reader;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Εισαγωγή CSV με βάση το {@link CsvPlan} που έφτιαξε ο χρήστης: μόνο οι στήλες που διάλεξε,
 * με τον τύπο που διάλεξε, με τα ονόματα που διάλεξε.
 * <p>
 * <b>Γιατί δεν περνά από JSON:</b> η παλιά διαδρομή έφτιαχνε για κάθε γραμμή ένα {@code HashMap},
 * το μετέτρεπε σε {@code JsonNode}, το ξανασειριοποιούσε σε {@code String} και μετά διάβαζε από
 * το δέντρο τα πεδία ένα-ένα — τέσσερις αναπαραστάσεις των ίδιων δεδομένων, όλες σκουπίδια μέσα
 * σε ένα χιλιοστό. Εδώ η γραμμή του CSV γίνεται <b>απευθείας</b> Protobuf.
 * <p>
 * Η διάταξη ({@link Layout}) υπολογίζεται μία φορά πριν την πρώτη γραμμή: ανά γραμμή δεν γίνεται
 * ούτε αναζήτηση ονόματος, ούτε ανάλυση διαδρομής, ούτε δημιουργία map.
 */
@Service
public class CsvIngestService {

    private static final Logger log = LoggerFactory.getLogger(CsvIngestService.class);

    private final RecordSender sender;
    private final KafkaTopicService topicService;
    private final IngestMeters meters;

    public CsvIngestService(RecordSender sender, KafkaTopicService topicService, IngestMeters meters) {
        this.sender = sender;
        this.topicService = topicService;
        this.meters = meters;
    }

    /** Ό,τι χρειάζεται ανά γραμμή, προ-υπολογισμένο σε πίνακες: μηδέν αναζητήσεις στον βρόχο. */
    private static final class Layout {
        final int[] index;
        final String[] target;
        final CsvValues.Type[] type;
        final int timeIndex;
        final int keyIndex;
        final char decimal;
        final DateTimeFormatter timeFormat;
        final String[] names;

        Layout(CsvPlan plan, List<String> names) {
            this.names = names.toArray(new String[0]);
            List<CsvPlan.Column> cols = plan.columns();
            this.index = new int[cols.size()];
            this.target = new String[cols.size()];
            this.type = new CsvValues.Type[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                CsvPlan.Column c = cols.get(i);
                index[i] = names.indexOf(c.name());
                target[i] = c.target();
                type[i] = c.typeOrText();
            }
            this.timeIndex = plan.timeColumn() == null || plan.timeColumn().isBlank()
                    ? -1 : names.indexOf(plan.timeColumn());
            this.keyIndex = plan.keyColumn() == null || plan.keyColumn().isBlank()
                    ? -1 : names.indexOf(plan.keyColumn());
            this.decimal = plan.decimalChar();
            this.timeFormat = plan.timeFormatter();
        }
    }

    /**
     * @param in   η ροή του CSV· κλείνεται από εδώ
     * @param plan τι διάλεξε ο χρήστης
     */
    public IngestResult ingest(Reader in, CsvPlan plan) throws Exception {
        String sourceId = plan.sourceId() != null && !plan.sourceId().isBlank() ? plan.sourceId() : "csv";
        RecordSender.Session session = null;
        int read = 0;
        int skipped = 0;
        int noTime = 0;
        String topic = null;

        try (CsvReader csv = new CsvReader(in, plan.delimiterChar())) {
            String[] first = nextNonBlank(csv);
            if (first == null) {
                throw new IllegalArgumentException("Το αρχείο δεν έχει καμία γραμμή με δεδομένα.");
            }
            List<String> names = plan.header() ? headerNames(first) : syntheticNames(first.length);
            plan.validate(names);
            Layout layout = new Layout(plan, names);

            // Το topic ελέγχεται ΜΕΤΑ το validate: ένα λάθος όνομα στήλης δεν πρέπει να φτιάξει topic.
            topic = topicService.resolve(plan.topic(), plan.createTopicIfMissing(), plan.partitions());
            session = sender.open(topic);

            String[] row = plan.header() ? nextNonBlank(csv) : first;
            boolean checkedTime = false;

            while (row != null) {
                try {
                    if (!checkedTime) {
                        assertTimeReadable(layout, row, plan);
                        checkedTime = true;
                    }
                    if (send(session, sourceId, layout, row, plan.keepRaw())) {
                        noTime++;
                    }
                    read++;
                } catch (IllegalArgumentException e) {
                    throw e; // λάθος στο plan: δεν έχει νόημα να συνεχίσουμε για εκατομμύρια γραμμές
                } catch (Exception e) {
                    skipped++;
                    if (skipped <= 5) { // όχι μία γραμμή log ανά χαλασμένη γραμμή σε αρχείο 10 GB
                        log.warn("Γραμμή {} του CSV προσπεράστηκε: {}", csv.lineNumber(), e.getMessage());
                    }
                }
                row = nextNonBlank(csv);
            }
        }

        if (noTime > 0) {
            log.warn("{} από {} γραμμές δεν είχαν διαβάσιμο χρόνο στη στήλη '{}' και πήραν την ώρα "
                    + "της εισαγωγής.", noTime, read, plan.timeColumn());
        }
        return meters.finish("csv", read + skipped, read, skipped, session);
    }

    /**
     * Ο έλεγχος που σώζει μια εισαγωγή εκατομμυρίων γραμμών: αν δηλώθηκε στήλη χρόνου αλλά η
     * <b>πρώτη</b> γραμμή δεν διαβάζεται, σταματάμε πριν σταλεί το πρώτο μήνυμα. Αλλιώς όλα τα
     * ιστορικά δεδομένα θα έπαιρναν σιωπηλά την ώρα της εισαγωγής — μια βάση γεμάτη δεδομένα που
     * δείχνουν όλα «τώρα» και δεν φαίνεται πουθενά ότι κάτι πήγε λάθος.
     */
    private static void assertTimeReadable(Layout layout, String[] row, CsvPlan plan) {
        if (layout.timeIndex < 0 || layout.timeIndex >= row.length) {
            return;
        }
        String raw = row[layout.timeIndex];
        if (CsvValues.isMissing(raw) || eventTime(layout, row) != null) {
            return;
        }
        throw new IllegalArgumentException("Η στήλη χρόνου '" + plan.timeColumn() + "' έχει τιμή '" + raw
                + "' που δεν διαβάζεται ως χρόνος"
                + (plan.timeFormat() == null ? ". Δώσε timeFormat, π.χ. \"dd/MM/yyyy HH:mm\"."
                                             : " με το timeFormat '" + plan.timeFormat() + "'."));
    }

    private static Long eventTime(Layout layout, String[] row) {
        if (layout.timeIndex < 0 || layout.timeIndex >= row.length) {
            return null;
        }
        String raw = row[layout.timeIndex];
        if (CsvValues.isMissing(raw)) {
            return null;
        }
        return layout.timeFormat != null ? CsvValues.parseTime(raw, layout.timeFormat) : CsvValues.parseTime(raw);
    }

    /**
     * Χτίζει το Protobuf record από τη γραμμή και το στέλνει.
     *
     * @return {@code true} αν η γραμμή δεν είχε διαβάσιμο χρόνο (για να μετρηθεί)
     */
    private boolean send(RecordSender.Session session, String sourceId, Layout layout, String[] row,
                         boolean keepRaw) {
        IngestedRecordProto.IngestedRecord.Builder b = IngestedRecordProto.IngestedRecord.newBuilder()
                .setSourceId(sourceId);

        for (int c = 0; c < layout.index.length; c++) {
            int i = layout.index[c];
            if (i < 0 || i >= row.length) {
                continue; // κοντή γραμμή: λείπουν οι τελευταίες στήλες
            }
            String raw = row[i];
            if (CsvValues.isMissing(raw)) {
                continue; // ΚΕΝΟ ≠ ΜΗΔΕΝ: το πεδίο απλώς δεν υπάρχει σε αυτή την εγγραφή
            }
            Value v = value(layout.type[c], raw, layout.decimal);
            if (v != null) {
                b.putFields(layout.target[c], v);
            }
        }
        if (keepRaw) {
            b.setPayloadJson(rawJson(layout.names, row));
        }

        String key = layout.keyIndex >= 0 && layout.keyIndex < row.length ? row[layout.keyIndex] : null;
        Long eventTime = eventTime(layout, row);

        session.send(eventTime, emptyToNull(key), b.build());
        return layout.timeIndex >= 0 && eventTime == null;
    }

    /**
     * Η τιμή με τον τύπο που δήλωσε ο χρήστης. Μια τιμή που δεν ταιριάζει στον τύπο
     * <b>παραλείπεται</b> αντί να μπει ως κείμενο: το ίδιο πεδίο με δύο τύπους δίνει δύο γραμμές
     * στο {@code field_catalog} και χαλάει και τη χρονοσειρά, που δέχεται μόνο αριθμούς.
     */
    private static Value value(CsvValues.Type type, String raw, char decimal) {
        switch (type) {
            case NUMBER -> {
                double d = CsvValues.parseNumber(raw, decimal);
                return Double.isNaN(d) ? null : Value.newBuilder().setNumberValue(d).build();
            }
            case BOOL -> {
                Boolean b = CsvValues.parseBool(raw);
                return b == null ? null : Value.newBuilder().setBoolValue(b).build();
            }
            case TIMESTAMP -> {
                // Ως πεδίο ο χρόνος μπαίνει σε epoch millis, δηλαδή ως αριθμός: έτσι είναι
                // ταξινομήσιμος και συγκρίσιμος, σε αντίθεση με κείμενο σε άγνωστη μορφή.
                Long t = CsvValues.parseTime(raw);
                return t == null ? null : Value.newBuilder().setNumberValue(t).build();
            }
            default -> {
                return Value.newBuilder().setStringValue(raw).build();
            }
        }
    }

    /**
     * Η γραμμή ως JSON, όταν ο χρήστης θέλει να κρατηθεί το ωμό δεδομένο. Χτίζεται με
     * {@code StringBuilder} και όχι με Jackson: είναι ένα επίπεδο αντικείμενο από Strings, και η
     * διαδρομή map → δέντρο → κείμενο κόστιζε περισσότερο από όλη την υπόλοιπη γραμμή μαζί.
     */
    static String rawJson(String[] names, String[] row) {
        StringBuilder sb = new StringBuilder(64 + row.length * 16);
        sb.append('{');
        for (int i = 0; i < row.length && i < names.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            escape(sb, names[i]);
            sb.append(':');
            escape(sb, row[i]);
        }
        return sb.append('}').toString();
    }

    private static void escape(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private static String[] nextNonBlank(CsvReader csv) throws java.io.IOException {
        String[] row;
        while ((row = csv.next()) != null) {
            if (!CsvReader.isBlank(row)) {
                return row;
            }
        }
        return null;
    }

    /**
     * Τα ονόματα των στηλών, καθαρισμένα από BOM και κενά. Δύο στήλες με το ίδιο όνομα θα έγραφαν
     * η μία πάνω στην άλλη μέσα στο {@code fields} map, άρα η δεύτερη παίρνει κατάληξη.
     */
    static List<String> headerNames(String[] header) {
        List<String> names = new ArrayList<>(header.length);
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (int i = 0; i < header.length; i++) {
            String n = CsvReader.stripBom(header[i]).strip();
            if (n.isEmpty()) {
                n = "col_" + (i + 1);
            }
            if (!seen.add(n)) {
                int k = 2;
                while (!seen.add(n + "_" + k)) {
                    k++;
                }
                n = n + "_" + k;
            }
            names.add(n);
        }
        return names;
    }

    static List<String> syntheticNames(int width) {
        List<String> names = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            names.add("col_" + (i + 1));
        }
        return names;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
