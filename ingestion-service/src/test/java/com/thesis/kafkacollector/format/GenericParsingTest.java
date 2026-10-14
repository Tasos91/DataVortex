package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ο σωλήνας πρέπει να δέχεται δομές που δεν τις σχεδίασε κανείς για εμάς: δεδομένα θαμμένα μέσα σε
 * επίπεδα απάντησης, λίστες μέσα σε λίστες, XML με χαρακτηριστικά, μονάδες που θέλουν μετατροπή.
 */
class GenericParsingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static List<JsonNode> records(String json, String path) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        RecordPath.of(path).forEach(MAPPER.readTree(json), out::add);
        return out;
    }

    // ------------------------------------------------------------- RecordPath

    @Test
    @DisplayName("βρίσκει εγγραφές θαμμένες μέσα σε επίπεδα απάντησης")
    void nestedRecords() throws Exception {
        String json = """
                {"status": "ok", "page": 3,
                 "data": {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}}""";
        List<JsonNode> records = records(json, "$.data.items[*]");

        assertEquals(3, records.size());
        assertEquals(1, records.get(0).get("id").asInt());
    }

    @Test
    @DisplayName("ξεδιπλώνει λίστες μέσα σε λίστες: 2 σταθμοί × 2 μετρήσεις = 4 εγγραφές")
    void nestedArraysExplode() throws Exception {
        String json = """
                {"stations": [
                   {"id": "ATH", "readings": [{"t": 20}, {"t": 21}]},
                   {"id": "SKG", "readings": [{"t": 15}, {"t": 16}]}]}""";
        List<JsonNode> records = records(json, "$.stations[*].readings[*]");

        assertEquals(4, records.size());
        assertEquals(20, records.get(0).get("t").asInt());
        assertEquals(16, records.get(3).get("t").asInt());
    }

    @Test
    @DisplayName("δομή με δυναμικά κλειδιά: κάθε τιμή του αντικειμένου είναι εγγραφή")
    void dynamicKeys() throws Exception {
        String json = """
                {"readings": {"ATH": {"t": 20}, "SKG": {"t": 15}, "PAT": {"t": 22}}}""";
        List<JsonNode> records = records(json, "$.readings[*]");

        assertEquals(3, records.size());
        assertEquals(20, records.get(0).get("t").asInt());
    }

    @Test
    @DisplayName("χωρίς διαδρομή: πίνακας σημαίνει πολλές εγγραφές, αντικείμενο μία")
    void rootDefaults() throws Exception {
        assertEquals(2, records("[{\"a\":1},{\"a\":2}]", null).size());
        assertEquals(1, records("{\"a\":1}", null).size());
        assertEquals(1, records("{\"a\":1}", "$").size());
    }

    @Test
    @DisplayName("διαδρομή που δεν υπάρχει: καμία εγγραφή, κανένα σφάλμα")
    void missingPath() throws Exception {
        assertTrue(records("{\"a\":1}", "$.δεν.υπάρχει[*]").isEmpty());
    }

    @Test
    @DisplayName("άκυρη σύνταξη πιάνεται αμέσως, όχι μετά από εκατομμύρια κενές εγγραφές")
    void invalidSyntax() {
        assertThrows(IllegalArgumentException.class, () -> RecordPath.of("$.items[*"));
        assertThrows(IllegalArgumentException.class, () -> RecordPath.of("$.items[abc]"));
        assertThrows(IllegalArgumentException.class, () -> RecordPath.of("$..items"));
    }

    // ------------------------------------------------------------- FieldSpec

    @Test
    @DisplayName("μετασχηματισμός μονάδων: Fahrenheit σε Celsius στην είσοδο, μία φορά")
    void scaleAndOffset() throws Exception {
        JsonNode record = MAPPER.readTree("{\"temp_f\": 212.0}");
        FieldSpec.Compiled field = new FieldSpec("TempC", "$.temp_f", "number", 0.5556, -17.78, null, null)
                .compile();

        Value value = field.read(record, null);
        assertNotNull(value);
        assertEquals(100.0, value.getNumberValue(), 0.05, "οι 212°F είναι 100°C");
    }

    @Test
    @DisplayName("πεδίο από τη ΡΙΖΑ του εγγράφου, ενώ η εγγραφή είναι βαθιά μέσα")
    void fieldFromRoot() throws Exception {
        JsonNode root = MAPPER.readTree("""
                {"station": {"id": "ATH"}, "readings": [{"t": 20}, {"t": 21}]}""");

        FieldSpec.Compiled station = FieldSpec.of("Station", "$$.station.id", "text").compile();
        FieldSpec.Compiled temp = FieldSpec.of("Temp", "$.t", "number").compile();
        assertTrue(station.fromRoot());

        List<String> seen = new ArrayList<>();
        RecordPath.of("$.readings[*]").forEachWithScope(root, (record, scope) -> {
            assertEquals("ATH", station.read(record, scope).getStringValue());
            seen.add(String.valueOf((int) temp.read(record, scope).getNumberValue()));
        });
        assertEquals(List.of("20", "21"), seen);
    }

    @Test
    @DisplayName("προεπιλογή όταν το πεδίο λείπει· χωρίς αυτήν το πεδίο απλώς δεν υπάρχει")
    void defaultValue() throws Exception {
        JsonNode record = MAPPER.readTree("{\"a\": 1}");
        assertNull(FieldSpec.of("B", "$.b", "number").compile().read(record, null));

        FieldSpec.Compiled withDefault = new FieldSpec("B", "$.b", "number", null, null, null, "0")
                .compile();
        assertEquals(0.0, withDefault.read(record, null).getNumberValue());
    }

    @Test
    @DisplayName("ο τύπος που δηλώνεται κερδίζει: αριθμός μέσα σε κείμενο γίνεται αριθμός")
    void declaredTypeWins() throws Exception {
        JsonNode record = MAPPER.readTree("{\"n\": \"87.5\", \"b\": \"yes\"}");

        assertEquals(Value.KindCase.NUMBER_VALUE,
                FieldSpec.of("N", "$.n", "number").compile().read(record, null).getKindCase());
        assertEquals(Value.KindCase.BOOL_VALUE,
                FieldSpec.of("B", "$.b", "bool").compile().read(record, null).getKindCase());
        // auto: ό,τι λέει η ίδια η τιμή — εδώ κείμενο
        assertEquals(Value.KindCase.STRING_VALUE,
                FieldSpec.of("N", "$.n", null).compile().read(record, null).getKindCase());
    }

    @Test
    @DisplayName("τιμή που δεν ταιριάζει στον τύπο παραλείπεται, δεν μπαίνει ως κείμενο")
    void wrongTypeSkipped() throws Exception {
        JsonNode record = MAPPER.readTree("{\"n\": \"oops\"}");
        assertNull(FieldSpec.of("N", "$.n", "number").compile().read(record, null),
                "το ίδιο πεδίο με δύο τύπους δίνει δύο γραμμές στο field_catalog");
    }

    // ------------------------------------------------------------- XML

    @Test
    @DisplayName("XML: στοιχεία, χαρακτηριστικά και επαναλαμβανόμενα αδέρφια")
    void xmlStructure() throws Exception {
        String xml = """
                <stations>
                  <station id="ATH" name="Αθήνα">
                    <reading ts="2026-09-01T10:00:00Z"><temp>23.5</temp><hum>65</hum></reading>
                    <reading ts="2026-09-01T11:00:00Z"><temp>24.1</temp><hum>63</hum></reading>
                  </station>
                </stations>""";
        List<JsonNode> records = new ArrayList<>();
        int n = XmlRecords.forEach(new StringReader(xml), "reading", records::add);

        assertEquals(2, n);
        assertEquals("23.5", records.get(0).get("temp").asText());
        assertEquals("2026-09-01T10:00:00Z", records.get(0).get("@ts").asText(),
                "τα χαρακτηριστικά γίνονται πεδία με πρόθεμα @");
    }

    @Test
    @DisplayName("XML: το ίδιο όνομα δύο φορές γίνεται λίστα")
    void xmlRepeatedBecomesArray() throws Exception {
        String xml = "<r><tag>a</tag><tag>b</tag><tag>c</tag></r>";
        List<JsonNode> records = new ArrayList<>();
        XmlRecords.forEach(new StringReader(xml), "r", records::add);

        assertTrue(records.get(0).get("tag").isArray());
        assertEquals(3, records.get(0).get("tag").size());
    }

    @Test
    @DisplayName("XML με εξωτερική οντότητα δεν διαβάζει αρχεία του δίσκου")
    void xmlExternalEntityIsBlocked() {
        String evil = """
                <?xml version="1.0"?>
                <!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>
                <r><v>&x;</v></r>""";
        List<JsonNode> records = new ArrayList<>();
        // Με το DTD κλειστό, ο parser απορρίπτει το έγγραφο αντί να διαβάσει το αρχείο.
        assertThrows(Exception.class, () -> XmlRecords.forEach(new StringReader(evil), "r", records::add));
    }

    // ------------------------------------------------------------- streaming JSON

    @Test
    @DisplayName("το JSON διαβάζεται σε ροή όταν η διαδρομή το επιτρέπει")
    void streamingJson() throws Exception {
        assertTrue(JsonRecords.isStreamable("$.data.items[*]"));
        assertTrue(JsonRecords.isStreamable("$"));
        assertTrue(JsonRecords.isStreamable(null));
        assertFalse(JsonRecords.isStreamable("$.a[*].b[*]"), "δύο ξεδιπλώματα θέλουν ολόκληρο το δέντρο");

        String json = """
                {"meta": {"huge": "....."}, "data": {"items": [{"a":1},{"a":2},{"a":3}]}}""";
        List<JsonNode> records = new ArrayList<>();
        int n = JsonRecords.forEach(new StringReader(json), "$.data.items[*]", MAPPER, records::add);

        assertEquals(3, n);
        assertEquals(3, records.get(2).get("a").asInt());
    }

    // ------------------------------------------------------------- ανίχνευση μορφής

    @Test
    @DisplayName("η μορφή βρίσκεται από το περιεχόμενο, όχι από την κατάληξη")
    void formatDetection() {
        assertEquals(IngestSpec.Format.JSON, IngestSpec.Format.detect("x.txt", "  {\"a\":1}"));
        assertEquals(IngestSpec.Format.JSON, IngestSpec.Format.detect("x.dat", "[{\"a\":1}]"));
        assertEquals(IngestSpec.Format.XML, IngestSpec.Format.detect("x.txt", "<?xml version=\"1.0\"?><r/>"));
        assertEquals(IngestSpec.Format.CSV, IngestSpec.Format.detect("x.json", "a,b\n1,2"),
                "το περιεχόμενο κερδίζει την κατάληξη");
        assertEquals(IngestSpec.Format.NDJSON, IngestSpec.Format.detect("x", "{\"a\":1}\n{\"a\":2}\n"));
        assertEquals(IngestSpec.Format.XML, IngestSpec.Format.parse("xml"));
        assertNull(IngestSpec.Format.parse("auto"));
        assertThrows(IllegalArgumentException.class, () -> IngestSpec.Format.parse("parquet"));
    }

    // ------------------------------------------------------------- επιθεώρηση

    @Test
    @DisplayName("η επιθεώρηση βρίσκει μόνη της πού είναι οι εγγραφές μέσα στην απάντηση")
    void inspectorFindsRecords() throws Exception {
        String json = """
                {"status": "ok", "took": 12,
                 "result": {"rows": [
                    {"ts": "2026-09-01T10:00:00Z", "host": "node-1", "cpu": 87.5, "mem": 12.0},
                    {"ts": "2026-09-01T11:00:00Z", "host": "node-1", "cpu": 45.2, "mem": 11.5},
                    {"ts": "2026-09-01T10:00:00Z", "host": "node-2", "cpu": 12.1, "mem": 8.0},
                    {"ts": "2026-09-01T11:00:00Z", "host": "node-2", "cpu": 15.9, "mem": 8.2}]}}""";
        StructureInspector.Profile profile =
                StructureInspector.inspect(json, IngestSpec.Format.JSON, "metrics.json", MAPPER);

        assertEquals("$.result.rows[*]", profile.recordPath());
        assertEquals(4, profile.recordsSampled());

        IngestSpec suggestion = profile.suggestion();
        assertEquals("metrics", suggestion.sourceId());
        assertEquals("ingest.metrics", suggestion.topic());
        assertEquals("$.ts", suggestion.timeField());
        assertEquals("$.host", suggestion.keyField(), "δύο διακριτές τιμές σε τέσσερις γραμμές: οντότητα");
        assertTrue(suggestion.fields().stream().anyMatch(f -> f.name().equals("cpu")));
        assertTrue(suggestion.fields().stream().noneMatch(f -> f.path().equals("$.ts")),
                "ο χρόνος ζει στο event_time, δεν διπλασιάζεται ως πεδίο");
    }

    @Test
    @DisplayName("η ταυτότητα της οντότητας ζει στον ΓΟΝΕΑ και διαβάζεται με $^")
    void parentFields() throws Exception {
        // «Οντότητα με λίστα μετρήσεων» είναι η πιο φυσική δομή δεδομένων που υπάρχει. Το όνομα
        // του σταθμού είναι ΕΞΩ από τη μέτρηση· χωρίς πρόσβαση στον γονέα θα χανόταν, και μαζί
        // του το κλειδί partition και η ετικέτα.
        JsonNode root = MAPPER.readTree("""
                {"stations": [
                   {"id": "ATH", "readings": [{"t": 20}, {"t": 21}]},
                   {"id": "SKG", "readings": [{"t": 15}]}]}""");

        FieldSpec.Compiled station = FieldSpec.of("Station", "$^.id", "text").compile();
        FieldSpec.Compiled temp = FieldSpec.of("Temp", "$.t", "number").compile();
        assertEquals(1, station.parentLevels());

        List<String> pairs = new ArrayList<>();
        RecordPath.of("$.stations[*].readings[*]").forEachWithScope(root, (record, scope) ->
                pairs.add(station.read(record, scope).getStringValue() + "="
                        + (int) temp.read(record, scope).getNumberValue()));

        assertEquals(List.of("ATH=20", "ATH=21", "SKG=15"), pairs,
                "κάθε μέτρηση κρατά τον ΔΙΚΟ της σταθμό, όχι τον πρώτο");
    }

    @Test
    @DisplayName("όταν το ξεδίπλωμα είναι το τελευταίο βήμα, η εγγραφή δεν είναι πρόγονος του εαυτού της")
    void recordIsNotItsOwnParent() throws Exception {
        JsonNode root = MAPPER.readTree("{\"rows\": [{\"host\": \"a\"}, {\"host\": \"b\"}]}");
        FieldSpec.Compiled fromParent = FieldSpec.of("H", "$^.host", "text").compile();

        List<Value> values = new ArrayList<>();
        RecordPath.of("$.rows[*]").forEachWithScope(root, (record, scope) -> values.add(fromParent.read(record, scope)));

        // Δεν υπάρχει γονέας με «host»: το πεδίο λείπει, δεν παίρνει κρυφά την τιμή της εγγραφής
        assertEquals(2, values.size());
        assertTrue(values.stream().allMatch(java.util.Objects::isNull), values.toString());
    }

    @Test
    @DisplayName("η επιθεώρηση βρίσκει εγγραφές σε ΟΛΑ τα στοιχεία, όχι μόνο στο πρώτο")
    void inspectorExplodesAllBranches() throws Exception {
        String json = """
                {"data": {"stations": [
                   {"id": "ATH", "readings": [{"t": 20}, {"t": 21}, {"t": 22}]},
                   {"id": "SKG", "readings": [{"t": 15}, {"t": 16}, {"t": 17}]}]}}""";
        StructureInspector.Profile profile =
                StructureInspector.inspect(json, IngestSpec.Format.JSON, "w.json", MAPPER);

        assertEquals("$.data.stations[*].readings[*]", profile.recordPath(),
                "με [0] θα έπαιρνε μόνο τον πρώτο σταθμό");
        assertEquals(6, profile.recordsSampled());
        assertTrue(profile.fields().stream().anyMatch(f -> f.path().equals("$^.id")),
                "το id του σταθμού πρέπει να προταθεί ως πεδίο γονέα: " + profile.fields());
        assertEquals("$^.id", profile.suggestion().keyField(),
                "η οντότητα του γονέα είναι το φυσικό κλειδί partition");
    }

    @Test
    @DisplayName("η επιθεώρηση ξεδιπλώνει εμφωλευμένα πεδία σε διαδρομές")
    void inspectorFlattensNested() throws Exception {
        String json = """
                [{"id": 1, "sensor": {"temp": 20.5, "unit": "C"}},
                 {"id": 2, "sensor": {"temp": 21.0, "unit": "C"}},
                 {"id": 3, "sensor": {"temp": 19.8, "unit": "C"}}]""";
        StructureInspector.Profile profile =
                StructureInspector.inspect(json, IngestSpec.Format.JSON, "s.json", MAPPER);

        assertTrue(profile.fields().stream().anyMatch(f -> f.path().equals("$.sensor.temp")
                && f.type().equals("number")), profile.fields().toString());
        assertEquals(19.8, profile.fields().stream()
                .filter(f -> f.path().equals("$.sensor.temp")).findFirst().orElseThrow().min());
    }

    @Test
    @DisplayName("το όνομα του πεδίου είναι το τελευταίο κομμάτι της διαδρομής")
    void fieldNaming() {
        assertEquals("temp", StructureInspector.name("$.sensor.temp"));
        assertEquals("id", StructureInspector.name("$.id"));
        assertEquals("@ts", StructureInspector.name("$.@ts"));
    }
}
