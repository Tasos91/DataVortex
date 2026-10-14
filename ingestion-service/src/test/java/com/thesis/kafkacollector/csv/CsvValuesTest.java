package com.thesis.kafkacollector.csv;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

class CsvValuesTest {

    @Test
    @DisplayName("αριθμοί με τελεία και με κόμμα ως δεκαδικό")
    void numbers() {
        assertEquals(23.5, CsvValues.parseNumber("23.5", '.'));
        assertEquals(23.5, CsvValues.parseNumber("23,5", ','));
        assertEquals(-0.75, CsvValues.parseNumber("-0.75", '.'));
        assertEquals(150000.0, CsvValues.parseNumber("1.5e5", '.'));
    }

    @Test
    @DisplayName("διαχωριστικό χιλιάδων: 1.234,56 και 1,234.56 δίνουν την ίδια τιμή")
    void thousandsSeparator() {
        assertEquals(1234.56, CsvValues.parseNumber("1.234,56", ','));
        assertEquals(1234.56, CsvValues.parseNumber("1,234.56", '.'));
        assertEquals(1234567.0, CsvValues.parseNumber("1 234 567", '.'), "το Excel βάζει κενά");
    }

    @Test
    @DisplayName("ό,τι δεν είναι αριθμός γίνεται NaN, όχι μηδέν")
    void notNumbers() {
        assertTrue(Double.isNaN(CsvValues.parseNumber("abc", '.')));
        assertTrue(Double.isNaN(CsvValues.parseNumber("23%", '.')));
        assertTrue(Double.isNaN(CsvValues.parseNumber("12€", '.')));
        assertTrue(Double.isNaN(CsvValues.parseNumber("", '.')));
        assertTrue(Double.isNaN(CsvValues.parseNumber("-", '.')));
    }

    @Test
    @DisplayName("οι τιμές που σημαίνουν «δεν μετρήθηκε» αναγνωρίζονται — και ΔΕΝ γίνονται μηδέν")
    void missing() {
        for (String s : new String[]{"", " ", "NA", "n/a", "NULL", "-", "NaN", "?"}) {
            assertTrue(CsvValues.isMissing(s), s + " πρέπει να θεωρείται κενό");
        }
        assertFalse(CsvValues.isMissing("0"), "το μηδέν ΕΙΝΑΙ μέτρηση");
        assertFalse(CsvValues.isMissing("0.0"));
    }

    @Test
    @DisplayName("boolean στα αγγλικά και στα ελληνικά· το 1/0 μένει αριθμός")
    void booleans() {
        assertEquals(Boolean.TRUE, CsvValues.parseBool("TRUE"));
        assertEquals(Boolean.TRUE, CsvValues.parseBool("yes"));
        assertEquals(Boolean.TRUE, CsvValues.parseBool("ναι"));
        assertEquals(Boolean.FALSE, CsvValues.parseBool("No"));
        assertEquals(Boolean.FALSE, CsvValues.parseBool("όχι"));
        assertNull(CsvValues.parseBool("1"), "το 1 μπορεί να είναι μέτρηση, όχι σημαία");
        assertNull(CsvValues.parseBool("maybe"));
    }

    @Test
    @DisplayName("χρόνος: ISO, κενό αντί για T, σκέτη ημερομηνία, epoch s και ms")
    void times() {
        long expected = 1705314600000L; // 2024-01-15T10:30:00Z
        assertEquals(expected, CsvValues.parseTime("2024-01-15T10:30:00Z"));
        assertEquals(expected, CsvValues.parseTime("2024-01-15T10:30:00"));
        assertEquals(expected, CsvValues.parseTime("2024-01-15 10:30:00"), "SQL dump / Excel");
        assertEquals(expected, CsvValues.parseTime(String.valueOf(expected)));
        assertEquals(expected, CsvValues.parseTime(String.valueOf(expected / 1000)), "δευτερόλεπτα");
        assertEquals(1705276800000L, CsvValues.parseTime("2024-01-15"), "αρχή της ημέρας σε UTC");
    }

    @Test
    @DisplayName("χρόνος με offset ζώνης")
    void timeWithOffset() {
        assertEquals(CsvValues.parseTime("2024-01-15T10:30:00Z"),
                CsvValues.parseTime("2024-01-15T12:30:00+02:00"));
    }

    @Test
    @DisplayName("οι αμφίσημες μορφές ΔΕΝ μαντεύονται: θέλουν ρητό timeFormat")
    void ambiguousNeedsFormat() {
        assertNull(CsvValues.parseTime("15/01/2024 10:30"),
                "15/01 ή 01/15; μια λάθος εικασία μετακινεί δεδομένα μήνες μακριά");
        assertEquals(1705314600000L,
                CsvValues.parseTime("15/01/2024 10:30", DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")));
        assertEquals(1705276800000L,
                CsvValues.parseTime("15/01/2024", DateTimeFormatter.ofPattern("dd/MM/yyyy")));
    }

    @Test
    @DisplayName("ό,τι δεν είναι χρόνος δίνει null")
    void notTimes() {
        assertNull(CsvValues.parseTime("αύριο"));
        assertNull(CsvValues.parseTime("23.5"));
        assertNull(CsvValues.parseTime(""));
    }
}
