package com.thesis.datavortex.api.repo;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class CursorTest {

    @Test
    void survivesEncodeDecode() {
        Cursor c = new Cursor(Instant.parse("2026-10-14T12:34:56.789Z"), "ingest.ai", 3, 123456789L);
        Cursor back = Cursor.decode(c.encode());
        assertEquals(c, back);
    }

    @Test
    void topicWithSeparatorIsNotCorrupted() {
        // Το '|' χωρίζει τα πεδία· το topic είναι τελευταίο, άρα επιτρέπεται να το περιέχει.
        Cursor c = new Cursor(Instant.ofEpochMilli(1), "weird|topic|name", 0, 0);
        assertEquals("weird|topic|name", Cursor.decode(c.encode()).topic());
    }

    @Test
    void encodedCursorIsUrlSafe() {
        String encoded = new Cursor(Instant.now(), "ingest.weather", 5, 99).encode();
        assertFalse(encoded.contains("+") || encoded.contains("/") || encoded.contains("="));
    }

    @Test
    void garbageIsRejectedWithClearError() {
        assertThrows(IllegalArgumentException.class, () -> Cursor.decode("όχι-base64"));
        assertThrows(IllegalArgumentException.class, () -> Cursor.decode("YWJj"));
    }
}
