package com.thesis.kafkacollector.csv;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Η επιθεώρηση είναι αυτή που κάνει την επιλογή πεδίων δουλειά δύο κλικ αντί για διάβασμα
 * αρχείου σε άλλο πρόγραμμα. Αν μαντεύει λάθος, ο χρήστης διορθώνει — αλλά όταν μαντεύει σωστά
 * δεν χρειάζεται να πειράξει τίποτα.
 */
class CsvInspectorTest {

    private static CsvInspector.Column column(CsvInspector.Profile p, String name) {
        return p.columns().stream().filter(c -> c.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("δεν βρέθηκε στήλη " + name));
    }

    @Test
    @DisplayName("βρίσκει τύπους, ρόλους και φτιάχνει plan που δουλεύει χωρίς αλλαγές")
    void typesAndRoles() throws IOException {
        String csv = """
                timestamp,station,temp_c,humidity,note
                2024-01-15T10:00:00Z,athens,12.5,68,
                2024-01-15T11:00:00Z,athens,14.0,65,ok
                2024-01-15T10:00:00Z,patra,13.1,70,
                2024-01-15T11:00:00Z,patra,15.2,66,
                """;
        CsvInspector.Profile p = CsvInspector.inspect(csv, "weather-2024.csv");

        assertEquals(",", p.delimiter());
        assertTrue(p.hasHeader());
        assertEquals(4, p.rowsSampled());

        assertEquals("timestamp", column(p, "timestamp").type());
        assertEquals("time", column(p, "timestamp").role());
        assertEquals("number", column(p, "temp_c").type());
        assertEquals("metric", column(p, "temp_c").role());
        assertEquals("number", column(p, "humidity").type());
        // Δύο σταθμοί σε τέσσερις γραμμές: οντότητα, άρα καλό κλειδί partition
        assertEquals("key", column(p, "station").role());

        CsvPlan plan = p.suggestion();
        assertEquals("weather-2024", plan.sourceId());
        assertEquals("ingest.weather-2024", plan.topic(), "το topic ΠΡΕΠΕΙ να αρχίζει από ingest.");
        assertEquals("timestamp", plan.timeColumn());
        assertEquals("station", plan.keyColumn());
        assertTrue(plan.columns().stream().noneMatch(c -> c.name().equals("timestamp")),
                "η στήλη χρόνου ζει στο event_time, δεν διπλασιάζεται ως πεδίο");
        assertTrue(plan.columns().stream().anyMatch(c -> c.name().equals("temp_c") && "number".equals(c.type())));
    }

    @Test
    @DisplayName("ελληνικό Excel: διαχωριστικό ; και δεκαδικό κόμμα")
    void greekExcel() throws IOException {
        String csv = """
                ημερομηνία;σταθμός;θερμοκρασία
                2024-03-01 08:00:00;ΑΘΗΝΑ;12,5
                2024-03-01 09:00:00;ΑΘΗΝΑ;14,25
                2024-03-01 08:00:00;ΠΑΤΡΑ;11,8
                """;
        CsvInspector.Profile p = CsvInspector.inspect(csv, "καιρός.csv");
        assertEquals(";", p.delimiter());
        assertEquals("number", column(p, "θερμοκρασία").type());
        assertEquals(",", column(p, "θερμοκρασία").decimal());
        assertEquals(",", p.suggestion().decimal());
        // 12,5 / 14,25 / 11,8 — αν το κόμμα διαβαζόταν ως χιλιάδες, το max θα ήταν 1425
        assertEquals(11.8, column(p, "θερμοκρασία").min());
        assertEquals(14.25, column(p, "θερμοκρασία").max());
    }

    @Test
    @DisplayName("το διαχωριστικό κρίνεται από τη σταθερότητα των στηλών, όχι από τη συχνότητα")
    void delimiterByStability() throws IOException {
        // Περισσότερα κόμματα από ';' αλλά τα κόμματα είναι μέσα στο κείμενο
        String csv = """
                id;περιγραφή;τιμή
                1;"ένα, δύο, τρία";10
                2;"τέσσερα, πέντε";20
                """;
        assertEquals(";", CsvInspector.inspect(csv, null).delimiter());
        assertEquals('\t', CsvInspector.sniffDelimiter("a\tb\tc\n1\t2\t3\n"));
        assertEquals('|', CsvInspector.sniffDelimiter("a|b\n1|2\n"));
    }

    @Test
    @DisplayName("χωρίς επικεφαλίδες: οι στήλες γίνονται col_1..col_N και υπάρχει σημείωση")
    void noHeader() throws IOException {
        CsvInspector.Profile p = CsvInspector.inspect("1,2.5\n2,3.5\n3,4.5\n", "d.csv");
        assertFalse(p.hasHeader());
        assertEquals(3, p.rowsSampled(), "καμία γραμμή δεν θυσιάστηκε για επικεφαλίδες");
        assertEquals("col_1", p.columns().get(0).name());
        assertFalse(p.notes().isEmpty());
    }

    @Test
    @DisplayName("ένα epoch γίνεται χρόνος μόνο αν το λέει και το όνομα της στήλης")
    void epochNeedsName() throws IOException {
        String csv = """
                ts,population
                1705314600,3100000
                1705318200,3200000
                1705321800,3300000
                """;
        CsvInspector.Profile p = CsvInspector.inspect(csv, null);
        assertEquals("timestamp", column(p, "ts").type());
        assertEquals("time", column(p, "ts").role());
        assertEquals("number", column(p, "population").type(),
                "3,1 εκατομμύρια δεν είναι ημερομηνία του 1970");
        assertEquals("metric", column(p, "population").role());
    }

    @Test
    @DisplayName("μετρά κενά και διαφορετικές τιμές· η ολόκληρη κενή στήλη παραλείπεται")
    void missingAndDistinct() throws IOException {
        String csv = """
                a,b,c
                1,x,
                2,,
                ,x,
                """;
        CsvInspector.Profile p = CsvInspector.inspect(csv, null);
        assertEquals(2, column(p, "a").present());
        assertEquals(1, column(p, "a").missing());
        assertEquals(1, column(p, "b").distinct());
        assertEquals("skip", column(p, "c").role());
        assertTrue(p.suggestion().columns().stream().noneMatch(x -> x.name().equals("c")));
    }

    @Test
    @DisplayName("προειδοποιεί όταν δεν υπάρχει χρόνος ή καμία μετρική")
    void warns() throws IOException {
        CsvInspector.Profile p = CsvInspector.inspect("name,city\nΝίκος,Αθήνα\nΜαρία,Πάτρα\n", null);
        assertTrue(p.notes().stream().anyMatch(n -> n.contains("χρόνος")), p.notes().toString());
        assertTrue(p.notes().stream().anyMatch(n -> n.contains("μετρικές")), p.notes().toString());
    }

    @Test
    @DisplayName("διπλά ονόματα στηλών δεν γράφουν το ένα πάνω στο άλλο")
    void duplicateHeaders() throws IOException {
        CsvInspector.Profile p = CsvInspector.inspect("v,v,v\n1,2,3\n", null);
        assertEquals(List.of("v", "v_2", "v_3"), p.columns().stream().map(CsvInspector.Column::name).toList());
    }

    @Test
    @DisplayName("το sourceId βγαίνει καθαρό από το όνομα του αρχείου")
    void sourceIdFromFileName() {
        assertEquals("weather-2024", CsvInspector.sourceIdFrom("/home/tasos/Weather 2024.csv"));
        assertEquals("ais-data", CsvInspector.sourceIdFrom("ais_data.CSV"));
        assertEquals("csv", CsvInspector.sourceIdFrom(null));
        assertEquals("csv", CsvInspector.sourceIdFrom("---.csv"));
    }

    @Test
    @DisplayName("άδειο αρχείο: καθαρό σφάλμα, όχι NullPointerException")
    void emptyFile() {
        assertThrows(IllegalArgumentException.class, () -> CsvInspector.inspect("", "x.csv"));
        assertThrows(IllegalArgumentException.class, () -> CsvInspector.inspect("\n\n", "x.csv"));
    }
}
