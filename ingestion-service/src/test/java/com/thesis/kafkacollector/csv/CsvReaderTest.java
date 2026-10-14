package com.thesis.kafkacollector.csv;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Κάθε test εδώ αντιστοιχεί σε ένα αρχείο που το παλιό {@code line.split(",")} θα το διάβαζε λάθος
 * — χωρίς να παραπονεθεί πουθενά.
 */
class CsvReaderTest {

    private static List<String[]> read(String text, char delimiter) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (CsvReader r = new CsvReader(new StringReader(text), delimiter)) {
            String[] row;
            while ((row = r.next()) != null) {
                if (!CsvReader.isBlank(row)) {
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    @Test
    @DisplayName("απλό CSV: τρεις στήλες, δύο γραμμές")
    void simple() throws IOException {
        List<String[]> rows = read("a,b,c\n1,2,3\n", ',');
        assertEquals(2, rows.size());
        assertArrayEquals(new String[]{"a", "b", "c"}, rows.get(0));
        assertArrayEquals(new String[]{"1", "2", "3"}, rows.get(1));
    }

    @Test
    @DisplayName("κόμμα μέσα σε εισαγωγικά ΔΕΝ χωρίζει πεδία")
    void commaInsideQuotes() throws IOException {
        List<String[]> rows = read("city,temp\n\"Αθήνα, Ελλάδα\",23.5\n", ',');
        assertArrayEquals(new String[]{"Αθήνα, Ελλάδα", "23.5"}, rows.get(1));
    }

    @Test
    @DisplayName("διπλό εισαγωγικό είναι ένα κυριολεκτικό \"")
    void escapedQuote() throws IOException {
        List<String[]> rows = read("note\n\"λέει \"\"γεια\"\" σε όλους\"\n", ',');
        assertEquals("λέει \"γεια\" σε όλους", rows.get(1)[0]);
    }

    @Test
    @DisplayName("αλλαγή γραμμής μέσα σε εισαγωγικά: μία λογική γραμμή")
    void newlineInsideQuotes() throws IOException {
        List<String[]> rows = read("id,text\n1,\"πρώτη\nδεύτερη\"\n2,ok\n", ',');
        assertEquals(3, rows.size(), "οι φυσικές γραμμές είναι 4, οι λογικές 3");
        assertEquals("πρώτη\nδεύτερη", rows.get(1)[1]);
        assertArrayEquals(new String[]{"2", "ok"}, rows.get(2));
    }

    @Test
    @DisplayName("CRLF: το \\r δεν μένει κολλημένο στο τελευταίο πεδίο")
    void crlf() throws IOException {
        List<String[]> rows = read("a,b\r\n1,2\r\n", ',');
        assertEquals("2", rows.get(1)[1]);
        assertEquals("b", rows.get(0)[1]);
    }

    @Test
    @DisplayName("BOM: η πρώτη στήλη δεν λέγεται \\uFEFFid")
    void bom() throws IOException {
        List<String[]> rows = read("﻿id,v\n1,2\n", ',');
        assertEquals("id", CsvReader.stripBom(rows.get(0)[0]));
    }

    @Test
    @DisplayName("ελληνικό CSV: διαχωριστικό ; και δεκαδικό κόμμα")
    void semicolonDelimiter() throws IOException {
        List<String[]> rows = read("σταθμός;θερμοκρασία\nΑθήνα;23,5\n", ';');
        assertArrayEquals(new String[]{"Αθήνα", "23,5"}, rows.get(1));
    }

    @Test
    @DisplayName("TSV")
    void tabDelimiter() throws IOException {
        List<String[]> rows = read("a\tb\n1\t2\n", '\t');
        assertArrayEquals(new String[]{"1", "2"}, rows.get(1));
    }

    @Test
    @DisplayName("κενά γύρω από μη-εισαγωγικό πεδίο φεύγουν, μέσα σε εισαγωγικά μένουν")
    void spaces() throws IOException {
        List<String[]> rows = read("a, b ,\"  c  \"\n", ',');
        assertArrayEquals(new String[]{"a", "b", "  c  "}, rows.get(0));
    }

    @Test
    @DisplayName("κοντή και άδεια γραμμή")
    void shortAndBlankRows() throws IOException {
        List<String[]> rows = read("a,b,c\n1,2\n\n4,5,6\n", ',');
        assertEquals(3, rows.size(), "η κενή γραμμή προσπερνιέται");
        assertEquals(2, rows.get(1).length, "η κοντή γραμμή μένει κοντή, δεν γεμίζει με κενά");
    }

    @Test
    @DisplayName("κενά πεδία: \"\" και σκέτο κόμμα")
    void emptyFields() throws IOException {
        List<String[]> rows = read("a,,\"\",d\n", ',');
        assertArrayEquals(new String[]{"a", "", "", "d"}, rows.get(0));
    }

    @Test
    @DisplayName("τελευταία γραμμή χωρίς newline")
    void noTrailingNewline() throws IOException {
        List<String[]> rows = read("a,b\n1,2", ',');
        assertEquals(2, rows.size());
        assertArrayEquals(new String[]{"1", "2"}, rows.get(1));
    }

    @Test
    @DisplayName("η αρίθμηση γραμμών μετρά λογικές γραμμές, για μηνύματα που βγάζουν νόημα")
    void lineNumbers() throws IOException {
        try (CsvReader r = new CsvReader(new StringReader("a\n\"δύο\nφυσικές\"\nc\n"), ',')) {
            r.next();
            assertEquals(1, r.lineNumber());
            r.next();
            assertEquals(2, r.lineNumber());
            r.next();
            assertEquals(3, r.lineNumber());
        }
    }

    @Test
    @DisplayName("εισαγωγικό στη μέση μη-εισαγωγικού πεδίου μένει κυριολεκτικό")
    void strayQuote() throws IOException {
        List<String[]> rows = read("5\" οθόνη,2\n", ',');
        assertEquals("5\" οθόνη", rows.get(0)[0]);
    }

    @Test
    @DisplayName("μεγάλο πεδίο πάνω από το buffer των 64 KB")
    void hugeField() throws IOException {
        String big = "x".repeat(200_000);
        List<String[]> rows = read("a,b\n" + big + ",2\n", ',');
        assertEquals(big.length(), rows.get(1)[0].length());
        assertEquals("2", rows.get(1)[1]);
    }
}
