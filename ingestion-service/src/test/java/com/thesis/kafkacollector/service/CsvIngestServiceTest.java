package com.thesis.kafkacollector.service;

import com.google.protobuf.Value;
import com.thesis.kafkacollector.csv.CsvPlan;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Το κρίσιμο test αυτής της φάσης: ότι μια στήλη που ο χρήστης δήλωσε αριθμητική φτάνει στο
 * Protobuf ως <b>αριθμός</b>.
 * <p>
 * Πριν, κάθε στήλη CSV γινόταν {@code string_value}. Ο writer γράφει στο {@code metric_points}
 * μόνο ό,τι έχει {@code jsonb_typeof = 'number'}, άρα κανένα CSV δεν παρήγαγε ποτέ μετρική: τα
 * δεδομένα μπαίνανε στη βάση και το UI έδειχνε άδειο γράφημα, χωρίς κανένα σφάλμα πουθενά.
 */
class CsvIngestServiceTest {

    /** Κρατά ό,τι στάλθηκε, χωρίς Kafka: μας ενδιαφέρει το μήνυμα, όχι η μεταφορά του. */
    private static final class CapturingTemplate extends KafkaTemplate<String, IngestedRecord> {
        final List<ProducerRecord<String, IngestedRecord>> sent = new ArrayList<>();

        CapturingTemplate() {
            super(new DefaultKafkaProducerFactory<>(Map.of()));
        }

        @Override
        public CompletableFuture<SendResult<String, IngestedRecord>> send(
                ProducerRecord<String, IngestedRecord> record) {
            sent.add(record);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void flush() {
            // τίποτα: δεν υπάρχει αληθινός producer
        }
    }

    /** Δεν υπάρχει broker στα unit tests, άρα ο έλεγχος topic απλώς επιστρέφει το όνομα. */
    private static final class NoopTopics extends KafkaTopicService {
        NoopTopics() {
            super(null);
        }

        @Override
        public String resolve(String requested, boolean createIfMissing, Integer partitions) {
            return requested != null ? requested : "ingest.test";
        }
    }

    private CapturingTemplate template;
    private CsvIngestService service;

    @BeforeEach
    void setUp() {
        template = new CapturingTemplate();
        service = new CsvIngestService(template, new NoopTopics(),
                new IngestMeters(template, new SimpleMeterRegistry()));
    }

    private static CsvPlan plan(String time, String key, String decimal, CsvPlan.Column... columns) {
        return new CsvPlan("test", "ingest.test", false, 1, ",", decimal, true,
                time, null, key, List.of(columns), false);
    }

    private static CsvPlan.Column col(String name, String type) {
        return new CsvPlan.Column(name, name, type);
    }

    private IngestResult ingest(String csv, CsvPlan plan) throws Exception {
        return service.ingest(new StringReader(csv), plan);
    }

    @Test
    @DisplayName("αριθμητική στήλη γίνεται number_value, όχι string_value")
    void numbersStayNumbers() throws Exception {
        IngestResult r = ingest("""
                ts,station,temp_c
                2024-01-15T10:00:00Z,athens,12.5
                """, plan("ts", "station", ".", col("temp_c", "number"), col("station", "text")));

        assertEquals(1, r.read());
        assertEquals(1, r.accepted());
        IngestedRecord record = template.sent.get(0).value();

        Value temp = record.getFieldsMap().get("temp_c");
        assertEquals(Value.KindCase.NUMBER_VALUE, temp.getKindCase(),
                "αν είναι string, το metric_points μένει άδειο και δεν υπάρχει χρονοσειρά");
        assertEquals(12.5, temp.getNumberValue());

        assertEquals(Value.KindCase.STRING_VALUE, record.getFieldsMap().get("station").getKindCase());
        assertEquals("athens", record.getFieldsMap().get("station").getStringValue());
    }

    @Test
    @DisplayName("μόνο οι στήλες του plan φτάνουν στο Kafka — οι υπόλοιπες ούτε ως bytes")
    void onlyChosenColumns() throws Exception {
        ingest("""
                ts,a,b,c
                2024-01-15T10:00:00Z,1,2,3
                """, plan("ts", null, ".", col("a", "number")));

        IngestedRecord record = template.sent.get(0).value();
        assertEquals(1, record.getFieldsCount());
        assertTrue(record.getFieldsMap().containsKey("a"));
        assertFalse(record.getFieldsMap().containsKey("b"));
    }

    @Test
    @DisplayName("μετονομασία: η στήλη temp_c ζει στο proto ως TempC")
    void rename() throws Exception {
        ingest("ts,temp_c\n2024-01-15T10:00:00Z,12.5\n",
                new CsvPlan("s", "ingest.test", false, 1, ",", ".", true, "ts", null, null,
                        List.of(new CsvPlan.Column("temp_c", "TempC", "number")), false));

        assertTrue(template.sent.get(0).value().getFieldsMap().containsKey("TempC"));
    }

    @Test
    @DisplayName("ο χρόνος της στήλης γίνεται timestamp του μηνύματος — αλλιώς δεν υπάρχει χρονοσειρά")
    void eventTimeFromColumn() throws Exception {
        ingest("ts,v\n2024-01-15T10:30:00Z,1\n", plan("ts", null, ".", col("v", "number")));
        assertEquals(1705314600000L, template.sent.get(0).timestamp());
    }

    @Test
    @DisplayName("το κλειδί βγαίνει από τη στήλη που διάλεξε ο χρήστης")
    void keyFromColumn() throws Exception {
        ingest("""
                ts,station,v
                2024-01-15T10:00:00Z,athens,1
                2024-01-15T10:00:00Z,patra,2
                """, plan("ts", "station", ".", col("v", "number")));

        assertEquals("athens", template.sent.get(0).key());
        assertEquals("patra", template.sent.get(1).key());
    }

    @Test
    @DisplayName("ΚΕΝΟ ≠ ΜΗΔΕΝ: το πεδίο λείπει, δεν γίνεται 0")
    void missingIsNotZero() throws Exception {
        ingest("""
                ts,v,w
                2024-01-15T10:00:00Z,,NA
                """, plan("ts", null, ".", col("v", "number"), col("w", "number")));

        IngestedRecord record = template.sent.get(0).value();
        assertEquals(0, record.getFieldsCount(),
                "ένα μηδέν θα έσερνε κάθε μέσο όρο προς τα κάτω σαν να ήταν μέτρηση");
    }

    @Test
    @DisplayName("τιμή που δεν είναι αριθμός σε αριθμητική στήλη παραλείπεται, δεν μπαίνει ως κείμενο")
    void wrongTypeIsSkipped() throws Exception {
        ingest("""
                ts,v
                2024-01-15T10:00:00Z,oops
                """, plan("ts", null, ".", col("v", "number")));

        assertEquals(0, template.sent.get(0).value().getFieldsCount(),
                "το ίδιο πεδίο με δύο τύπους δίνει δύο γραμμές στο field_catalog");
    }

    @Test
    @DisplayName("ελληνικό δεκαδικό: 23,5 είναι 23.5 και όχι 235")
    void greekDecimal() throws Exception {
        CsvPlan p = new CsvPlan("s", "ingest.test", false, 1, ";", ",", true, "ts", null, null,
                List.of(col("temp", "number")), false);
        service.ingest(new StringReader("ts;temp\n2024-01-15T10:00:00Z;23,5\n"), p);

        assertEquals(23.5, template.sent.get(0).value().getFieldsMap().get("temp").getNumberValue());
    }

    @Test
    @DisplayName("boolean στήλη γίνεται bool_value")
    void booleans() throws Exception {
        ingest("ts,active\n2024-01-15T10:00:00Z,yes\n", plan("ts", null, ".", col("active", "bool")));
        Value v = template.sent.get(0).value().getFieldsMap().get("active");
        assertEquals(Value.KindCase.BOOL_VALUE, v.getKindCase());
        assertTrue(v.getBoolValue());
    }

    @Test
    @DisplayName("ο ωμός payload δεν στέλνεται αν δεν ζητηθεί: διπλασιάζει bytes χωρίς λόγο")
    void rawPayloadIsOptional() throws Exception {
        ingest("ts,v\n2024-01-15T10:00:00Z,1\n", plan("ts", null, ".", col("v", "number")));
        assertEquals("", template.sent.get(0).value().getPayloadJson());

        template.sent.clear();
        CsvPlan withRaw = new CsvPlan("s", "ingest.test", false, 1, ",", ".", true, "ts", null, null,
                List.of(col("v", "number")), true);
        service.ingest(new StringReader("ts,v\n2024-01-15T10:00:00Z,1\n"), withRaw);
        assertEquals("{\"ts\":\"2024-01-15T10:00:00Z\",\"v\":\"1\"}",
                template.sent.get(0).value().getPayloadJson());
    }

    @Test
    @DisplayName("λάθος timeFormat πιάνεται στην ΠΡΩΤΗ γραμμή, πριν σταλεί οτιδήποτε")
    void badTimeFailsFast() throws Exception {
        CsvPlan p = new CsvPlan("s", "ingest.test", false, 1, ",", ".", true, "ts", null, null,
                List.of(col("v", "number")), false);
        String csv = "ts,v\n15/01/2024 10:30,1\n15/01/2024 11:30,2\n";

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> service.ingest(new StringReader(csv), p));
        assertTrue(e.getMessage().contains("timeFormat"), e.getMessage());
        assertTrue(template.sent.isEmpty(), "ούτε ένα μήνυμα δεν έφυγε με λάθος χρόνο");
    }

    @Test
    @DisplayName("με ρητό timeFormat η ίδια είσοδος περνά")
    void explicitTimeFormat() throws Exception {
        CsvPlan p = new CsvPlan("s", "ingest.test", false, 1, ",", ".", true, "ts",
                "dd/MM/yyyy HH:mm", null, List.of(col("v", "number")), false);
        service.ingest(new StringReader("ts,v\n15/01/2024 10:30,1\n"), p);

        assertEquals(1705314600000L, template.sent.get(0).timestamp());
    }

    @Test
    @DisplayName("λάθος όνομα στήλης: καθαρό σφάλμα με τη λίστα των υπαρκτών")
    void unknownColumn() {
        CsvPlan p = plan(null, null, ".", col("δεν_υπάρχει", "number"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ingest("a,b\n1,2\n", p));
        assertTrue(e.getMessage().contains("δεν_υπάρχει") && e.getMessage().contains("[a, b]"), e.getMessage());
    }

    @Test
    @DisplayName("καμία στήλη: σφάλμα που λέει τι να κάνει ο χρήστης")
    void noColumns() {
        CsvPlan p = plan(null, null, ".");
        assertThrows(IllegalArgumentException.class, () -> ingest("a,b\n1,2\n", p));
    }

    @Test
    @DisplayName("χωρίς επικεφαλίδες: οι στήλες λέγονται col_1..col_N και η πρώτη γραμμή είναι δεδομένο")
    void withoutHeader() throws Exception {
        CsvPlan p = new CsvPlan("s", "ingest.test", false, 1, ",", ".", false, null, null, null,
                List.of(col("col_2", "number")), false);
        IngestResult r = service.ingest(new StringReader("a,1\nb,2\n"), p);

        assertEquals(2, r.read(), "καμία γραμμή δεν θυσιάστηκε για επικεφαλίδες");
        assertEquals(1.0, template.sent.get(0).value().getFieldsMap().get("col_2").getNumberValue());
    }

    @Test
    @DisplayName("κοντή γραμμή: περνά με τα πεδία που έχει, δεν ρίχνει την εισαγωγή")
    void shortRow() throws Exception {
        IngestResult r = ingest("""
                ts,a,b
                2024-01-15T10:00:00Z,1,2
                2024-01-15T11:00:00Z,3
                """, plan("ts", null, ".", col("a", "number"), col("b", "number")));

        assertEquals(2, r.read());
        assertEquals(1, template.sent.get(1).value().getFieldsCount());
    }

    @Test
    @DisplayName("χωρίς στήλη χρόνου δεν μπαίνει timestamp: ο χρόνος γίνεται η ώρα της εισαγωγής")
    void noTimeColumn() throws Exception {
        ingest("a\n1\n", plan(null, null, ".", col("a", "number")));
        assertNull(template.sent.get(0).timestamp());
    }

    @Test
    @DisplayName("1000 γραμμές: όλες φτάνουν, με τη σειρά")
    void manyRows() throws Exception {
        StringBuilder csv = new StringBuilder("ts,v\n");
        for (int i = 0; i < 1000; i++) {
            csv.append(1705314600000L + i * 1000L).append(',').append(i).append('\n');
        }
        IngestResult r = ingest(csv.toString(), plan("ts", null, ".", col("v", "number")));

        assertEquals(1000, r.read());
        assertEquals(1000, r.accepted());
        assertEquals(0, r.failed());
        assertEquals(999.0, template.sent.get(999).value().getFieldsMap().get("v").getNumberValue());
    }
}
