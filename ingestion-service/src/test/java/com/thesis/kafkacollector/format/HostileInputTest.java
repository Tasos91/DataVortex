package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.csv.CsvReader;
import com.thesis.kafkacollector.csv.CsvValues;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ό,τι χειρότερο μπορεί να φτάσει στην είσοδο.
 * <p>
 * Ο κανόνας είναι ένας και απόλυτος: <b>τίποτα δεν ρίχνει τη διεργασία</b>. Είτε διαβάζεται, είτε
 * απορρίπτεται με καθαρό μήνυμα και η ροή συνεχίζει. Μια εισαγωγή που σκοτώνει τη διεργασία
 * παρασύρει μαζί της κάθε άλλη εισαγωγή που τρέχει εκείνη τη στιγμή — γι' αυτό κάθε περίπτωση εδώ
 * ελέγχει ότι επιστρέφουμε <i>κάτι</i>, ακόμα κι όταν αυτό το κάτι είναι σφάλμα.
 */
class HostileInputTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static List<JsonNode> read(String json, String path) throws Exception {
        List<JsonNode> out = new ArrayList<>();
        RecordPath.of(path).forEach(MAPPER.readTree(json), out::add);
        return out;
    }

    // ------------------------------------------------------------- κακόβουλα

    @Test
    @DisplayName("XML «billion laughs»: η εκθετική επέκταση οντοτήτων δεν γίνεται ποτέ")
    void billionLaughs() {
        // Η κλασική επίθεση άρνησης υπηρεσίας: δέκα επίπεδα × δέκα αναφορές = 10^10 χαρακτήρες.
        StringBuilder entities = new StringBuilder("<!ENTITY e0 \"χαχαχα\">");
        for (int i = 1; i <= 10; i++) {
            entities.append("<!ENTITY e").append(i).append(" \"");
            entities.append(("&e" + (i - 1) + ";").repeat(10));
            entities.append("\">");
        }
        String bomb = "<?xml version=\"1.0\"?><!DOCTYPE r [" + entities + "]><r><v>&e10;</v></r>";

        List<JsonNode> records = new ArrayList<>();
        // Με το DTD κλειστό ο parser απορρίπτει το έγγραφο. Το σημαντικό: τελειώνει, δεν κρεμάει.
        assertThrows(Exception.class, () -> XmlRecords.forEach(new StringReader(bomb), "r", records::add));
    }

    @Test
    @DisplayName("XXE: δεν διαβάζεται κανένα αρχείο του δίσκου")
    void externalEntity() {
        String evil = """
                <?xml version="1.0"?>
                <!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>
                <r><v>&x;</v></r>""";
        List<JsonNode> records = new ArrayList<>();
        assertThrows(Exception.class, () -> XmlRecords.forEach(new StringReader(evil), "r", records::add));
        assertTrue(records.stream().noneMatch(r -> r.toString().contains("root:")),
                "ούτε μερικώς δεν πρέπει να διαρρεύσει περιεχόμενο αρχείου");
    }

    @Test
    @DisplayName("βαθιά αναδρομή JSON: σφάλμα, όχι StackOverflow που ρίχνει τη διεργασία")
    void deeplyNestedJsonDoesNotKillTheProcess() {
        String deep = "[".repeat(50_000) + "]".repeat(50_000);
        // Το Jackson έχει δικό του όριο βάθους και πετά StreamConstraintsException. Το ζητούμενο
        // είναι ότι είναι Exception (πιάνεται) και όχι Error (που θα άφηνε τη JVM σε άγνωστη
        // κατάσταση): πιάνουμε Throwable για να φανεί ρητά ποια από τις δύο ήταν.
        Throwable t = assertThrows(Throwable.class, () -> MAPPER.readTree(deep));
        assertInstanceOf(Exception.class, t, "πρέπει να είναι Exception, όχι StackOverflowError: " + t);
    }

    @Test
    @DisplayName("βαθιά αναδρομή XML: τελειώνει, δεν κρεμάει")
    void deeplyNestedXml() {
        String deep = "<a>".repeat(5_000) + "<v>1</v>" + "</a>".repeat(5_000);
        List<JsonNode> records = new ArrayList<>();
        // Είτε διαβαστεί είτε απορριφθεί, το ζητούμενο είναι ότι επιστρέφει.
        assertDoesNotThrow(() -> {
            try {
                XmlRecords.forEach(new StringReader(deep), "v", records::add);
            } catch (Exception expected) {
                // αποδεκτό: καθαρό σφάλμα αντί για κατάρρευση
            }
        });
    }

    @Test
    @DisplayName("τεράστιο μοναδικό token: διαβάζεται ή απορρίπτεται, χωρίς κατάρρευση")
    void hugeSingleToken() throws Exception {
        String huge = "x".repeat(5_000_000);
        String json = "{\"big\": \"" + huge + "\"}";

        List<JsonNode> records = read(json, "$");
        assertEquals(1, records.size());
        assertEquals(huge.length(), records.get(0).get("big").asText().length());
    }

    // ------------------------------------------------------------- χαλασμένα

    @Test
    @DisplayName("JSON κομμένο στη μέση: καθαρό σφάλμα, όχι μισές εγγραφές")
    void truncatedJson() {
        assertThrows(Exception.class, () -> read("{\"a\": [1, 2, 3", "$"));
        assertThrows(Exception.class, () -> read("{\"a\":", "$"));
    }

    @Test
    @DisplayName("XML κομμένο στη μέση: όσες εγγραφές πρόλαβαν, μετά σφάλμα")
    void truncatedXml() {
        String cut = "<rs><r><v>1</v></r><r><v>2</v></r><r><v>3";
        List<JsonNode> records = new ArrayList<>();
        assertThrows(Exception.class, () -> XmlRecords.forEach(new StringReader(cut), "r", records::add));
        assertEquals(2, records.size(), "οι δύο ολόκληρες εγγραφές παραδόθηκαν πριν το σφάλμα");
    }

    @Test
    @DisplayName("CSV με ασυνεπές πλήθος στηλών: κάθε γραμμή περνά με όσα έχει")
    void raggedCsv() throws Exception {
        List<String[]> rows = new ArrayList<>();
        try (CsvReader r = new CsvReader(new StringReader("a,b,c\n1,2,3\n4,5\n6,7,8,9\n\n10\n"), ',')) {
            String[] row;
            while ((row = r.next()) != null) {
                if (!CsvReader.isBlank(row)) {
                    rows.add(row);
                }
            }
        }
        assertEquals(5, rows.size());
        assertEquals(3, rows.get(1).length);
        assertEquals(2, rows.get(2).length, "κοντή γραμμή μένει κοντή");
        assertEquals(4, rows.get(3).length, "μακριά γραμμή δεν κόβεται");
        assertEquals(1, rows.get(4).length);
    }

    @Test
    @DisplayName("ανάμεικτα τέλη γραμμής, BOM και χαρακτήρες ελέγχου")
    void mixedLineEndingsAndControlChars() throws Exception {
        String csv = "﻿a,b\r\n1,2\n3,4\r\n5,\u00006\n";
        List<String[]> rows = new ArrayList<>();
        try (CsvReader r = new CsvReader(new StringReader(csv), ',')) {
            String[] row;
            while ((row = r.next()) != null) {
                if (!CsvReader.isBlank(row)) {
                    rows.add(row);
                }
            }
        }
        assertEquals(4, rows.size());
        assertEquals("a", CsvReader.stripBom(rows.get(0)[0]), "το BOM φεύγει από το όνομα στήλης");
        assertEquals("2", rows.get(1)[1], "το \\r δεν μένει κολλημένο");
        assertTrue(rows.get(3)[1].contains("6"), "ο χαρακτήρας ελέγχου δεν ρίχνει τη γραμμή");
    }

    @Test
    @DisplayName("ελληνικά, emoji και επιτονισμένοι χαρακτήρες επιβιώνουν ακέραιοι")
    void unicodeSurvives() throws Exception {
        String json = "{\"όνομα\": \"Αθήνα 🌀\", \"note\": \"ñ ü 中文 👨‍👩‍👧‍👦\"}";
        JsonNode record = read(json, "$").get(0);

        assertEquals("Αθήνα 🌀", record.get("όνομα").asText());
        assertEquals("ñ ü 中文 👨‍👩‍👧‍👦", record.get("note").asText());

        FieldSpec.Compiled field = FieldSpec.of("Ν", "$.όνομα", "text").compile();
        assertEquals("Αθήνα 🌀", field.read(record, null).getStringValue());
    }

    @Test
    @DisplayName("τιμές που μοιάζουν αριθμοί αλλά δεν είναι, δεν γίνονται σιωπηλά μηδέν")
    void deceptiveNumbers() {
        for (String value : new String[]{"NaN", "Infinity", "1e999", "0x1F", "１２３", "- 5", "1,2,3"}) {
            double d = CsvValues.parseNumber(value);
            assertFalse(d == 0.0 && !"0".equals(value), "το '" + value + "' δεν πρέπει να γίνει μηδέν");
        }
        assertTrue(Double.isNaN(CsvValues.parseNumber("abc")));
        // Το 1e999 ξεφεύγει από το double: άπειρο, που ΔΕΝ είναι έγκυρο JSON — ο writer το κάνει null.
        assertTrue(Double.isInfinite(CsvValues.parseNumber("1e999")));
    }

    // ------------------------------------------------------------- ανομοιογένεια

    @Test
    @DisplayName("εγγραφές με εντελώς διαφορετικά πεδία: η καθεμία δίνει ό,τι έχει")
    void heterogeneousRecords() throws Exception {
        String json = """
                [{"a": 1, "b": "x"},
                 {"c": true},
                 {"a": "κείμενο τώρα", "d": {"nested": 5}},
                 {},
                 {"a": null}]""";
        List<JsonNode> records = read(json, "$");
        assertEquals(5, records.size());

        FieldSpec.Compiled a = FieldSpec.of("A", "$.a", "number").compile();
        assertEquals(1.0, a.read(records.get(0), null).getNumberValue());
        assertNull(a.read(records.get(1), null), "λείπει");
        assertNull(a.read(records.get(2), null), "υπάρχει αλλά δεν είναι αριθμός");
        assertNull(a.read(records.get(3), null), "άδεια εγγραφή");
        assertNull(a.read(records.get(4), null), "ρητό null");
    }

    @Test
    @DisplayName("πεδίο που αλλάζει τύπο ανά εγγραφή: ο δηλωμένος τύπος αποφασίζει")
    void typeChangesBetweenRecords() throws Exception {
        String ndjson = """
                {"v": 1}
                {"v": "2"}
                {"v": true}
                {"v": [1,2]}
                {"v": {"x": 1}}""";
        FieldSpec.Compiled asNumber = FieldSpec.of("V", "$.v", "number").compile();
        FieldSpec.Compiled asAuto = FieldSpec.of("V", "$.v", null).compile();

        List<JsonNode> records = new ArrayList<>();
        for (String line : ndjson.split("\n")) {
            records.add(MAPPER.readTree(line));
        }
        assertEquals(1.0, asNumber.read(records.get(0), null).getNumberValue());
        assertEquals(2.0, asNumber.read(records.get(1), null).getNumberValue(), "κείμενο που είναι αριθμός");
        assertNull(asNumber.read(records.get(2), null), "boolean δεν είναι αριθμός");
        assertNull(asNumber.read(records.get(3), null), "λίστα δεν είναι αριθμός");

        // auto: ό,τι λέει η τιμή — και οι σύνθετες δομές επιβιώνουν
        assertTrue(asAuto.read(records.get(3), null).hasListValue());
        assertTrue(asAuto.read(records.get(4), null).hasStructValue());
    }

    @Test
    @DisplayName("κενά σε κάθε θέση: ούτε ένα δεν γίνεται μηδέν")
    void emptinessEverywhere() throws Exception {
        String json = """
                {"a": null, "b": "", "c": "  ", "d": "NA", "e": "null", "f": "-"}""";
        JsonNode record = read(json, "$").get(0);
        for (String key : new String[]{"a", "b", "c", "d", "e", "f"}) {
            assertNull(FieldSpec.of("X", "$." + key, "number").compile().read(record, null),
                    "το '" + key + "' πρέπει να λείπει, όχι να γίνει 0");
        }
    }

    // ------------------------------------------------------------- ακραία μεγέθη

    @Test
    @DisplayName("εγγραφή με 1000 πεδία")
    void thousandFields() throws Exception {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(',');
            json.append("\"f").append(i).append("\":").append(i);
        }
        JsonNode record = read(json.append('}').toString(), "$").get(0);

        assertEquals(1000, record.size());
        assertEquals(999.0, FieldSpec.of("L", "$.f999", "number").compile()
                .read(record, null).getNumberValue());
    }

    @Test
    @DisplayName("φωλιά 12 επιπέδων: η διαδρομή φτάνει ως το τέρμα")
    void deepButValidNesting() throws Exception {
        StringBuilder json = new StringBuilder();
        StringBuilder path = new StringBuilder("$");
        for (int i = 0; i < 12; i++) {
            json.append("{\"l").append(i).append("\":");
            path.append(".l").append(i);
        }
        json.append("42");
        json.append("}".repeat(12));

        JsonNode record = read(json.toString(), "$").get(0);
        assertEquals(42.0, FieldSpec.of("Deep", path.toString(), "number").compile()
                .read(record, null).getNumberValue());
    }

    @Test
    @DisplayName("λίστες μέσα σε λίστες μέσα σε λίστες: τρία ξεδιπλώματα")
    void triplyNestedArrays() throws Exception {
        String json = """
                {"a": [
                  {"b": [ {"c": [{"v":1},{"v":2}]}, {"c": [{"v":3}]} ]},
                  {"b": [ {"c": [{"v":4},{"v":5},{"v":6}]} ]}
                ]}""";
        List<JsonNode> records = read(json, "$.a[*].b[*].c[*]");

        assertEquals(6, records.size());
        assertEquals(6, records.get(5).get("v").asInt());
    }

    @Test
    @DisplayName("η διαδρομή δείχνει σε βαθμωτά, όχι σε αντικείμενα: καμία κατάρρευση")
    void recordPathPointsToScalars() throws Exception {
        List<JsonNode> records = read("{\"a\": [1, 2, 3]}", "$.a[*]");
        assertEquals(3, records.size(), "τα βαθμωτά γίνονται εγγραφές — άδειες, αλλά χωρίς σφάλμα");
        assertNull(FieldSpec.of("X", "$.x", "number").compile().read(records.get(0), null));
    }
}
