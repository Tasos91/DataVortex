package com.thesis.datavortex.analytics.profile;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProfileSelectionTest {

    private final ProfileRegistry registry = new ProfileRegistry(
            List.of(new AiTelemetryProfile(), new WeatherProfile(), new GenericProfile()));

    @Test
    void fieldNamesRevealTheirMeaning() {
        assertEquals(FieldRoles.UTILIZATION, FieldRoles.of("GpuUtil"));
        assertEquals(FieldRoles.UTILIZATION, FieldRoles.of("cpu_load"));
        assertEquals(FieldRoles.LATENCY, FieldRoles.of("LatencyMs"));
        assertEquals(FieldRoles.LATENCY, FieldRoles.of("response_time"));
        assertEquals(FieldRoles.MEMORY, FieldRoles.of("MemGb"));
        assertEquals(FieldRoles.TEMPERATURE, FieldRoles.of("TempC"));
        assertEquals(FieldRoles.HUMIDITY, FieldRoles.of("Humidity"));
        assertEquals(FieldRoles.PRESSURE, FieldRoles.of("PressureHpa"));
        assertEquals(FieldRoles.WIND, FieldRoles.of("WindKmh"));
        assertEquals(FieldRoles.RAIN, FieldRoles.of("RainMm"));
        assertEquals(FieldRoles.ERRORS, FieldRoles.of("error_count"));
        assertEquals(FieldRoles.OTHER, FieldRoles.of("AlphaValue"));
    }

    @Test
    void greekFieldNamesRevealTheirMeaningToo() {
        // Ένα CSV από ελληνικό Excel έχει ελληνικές επικεφαλίδες. Χωρίς αυτό, κάθε τέτοιο αρχείο
        // έπεφτε στο generic και έχανε την εποχική βάση και τις μετεωρολογικές κατηγορίες.
        assertEquals(FieldRoles.TEMPERATURE, FieldRoles.of("θερμοκρασία"));
        assertEquals(FieldRoles.TEMPERATURE, FieldRoles.of("ΘΕΡΜΟΚΡΑΣΙΑ"), "κεφαλαία χωρίς τόνο");
        assertEquals(FieldRoles.HUMIDITY, FieldRoles.of("Υγρασία"));
        assertEquals(FieldRoles.WIND, FieldRoles.of("άνεμος"), "τελικό ς");
        assertEquals(FieldRoles.RAIN, FieldRoles.of("βροχή"));
        assertEquals(FieldRoles.PRESSURE, FieldRoles.of("πίεση"));
        assertEquals(FieldRoles.LATENCY, FieldRoles.of("καθυστέρηση_ms"));
        assertEquals(FieldRoles.UTILIZATION, FieldRoles.of("χρήση_cpu"));
        assertEquals(FieldRoles.ERRORS, FieldRoles.of("σφάλματα"));
        assertEquals(FieldRoles.MEMORY, FieldRoles.of("μνήμη"));
        assertEquals(FieldRoles.OTHER, FieldRoles.of("σημείωση"));
    }

    @Test
    void shortHintsMatchOnlyAtTheEnd() {
        // Το "ms" της καθυστέρησης χαρακτήριζε το "msgs" ως καθυστέρηση, και κέρδιζε επειδή το
        // LATENCY δηλώνεται πριν από το THROUGHPUT.
        assertEquals(FieldRoles.THROUGHPUT, FieldRoles.of("msgs"));
        assertEquals(FieldRoles.THROUGHPUT, FieldRoles.of("msgs_per_sec"));
        assertEquals(FieldRoles.LATENCY, FieldRoles.of("latency_ms"));
        assertEquals(FieldRoles.HUMIDITY, FieldRoles.of("rh"));
        assertEquals(FieldRoles.THROUGHPUT, FieldRoles.of("throughput"), "το rh δεν κρύβεται μέσα του");
    }

    @Test
    void greekWeatherCsvPicksTheWeatherProfile() {
        DataProfile profile = registry.resolve("weather-gr",
                List.of("θερμοκρασία", "υγρασία", "άνεμος", "βροχή"), "auto");
        assertEquals("weather", profile.id());

        DataProfile byName = registry.resolve("σταθμός-αθήνας",
                List.of("θερμοκρασία", "υγρασία"), "auto");
        assertEquals("weather", byName.id());
    }

    @Test
    void telemetryFieldsPickTheTelemetryProfile() {
        DataProfile profile = registry.resolve("gpu-farm",
                List.of("GpuUtil", "LatencyMs", "MemGb", "Throughput"), "auto");
        assertEquals("ai-telemetry", profile.id());
    }

    @Test
    void weatherFieldsPickTheWeatherProfile() {
        DataProfile profile = registry.resolve("station-42",
                List.of("TempC", "Humidity", "PressureHpa", "WindKmh", "RainMm"), "auto");
        assertEquals("weather", profile.id());
    }

    @Test
    void temperatureAloneIsNotEnoughToClaimWeather() {
        // Η θερμοκρασία υπάρχει και στην τηλεμετρία ενός μηχανήματος.
        DataProfile profile = registry.resolve("gpu-1", List.of("TempC", "GpuUtil"), "auto");
        assertEquals("ai-telemetry", profile.id());
    }

    @Test
    void unknownFieldsFallBackToGeneric() {
        DataProfile profile = registry.resolve("sensor-x",
                List.of("AlphaValue", "BetaValue", "GammaValue"), "auto");
        assertEquals("generic", profile.id());
    }

    @Test
    void noFieldsAtAllStillResolves() {
        assertEquals("generic", registry.resolve("whatever", List.of(), "auto").id());
    }

    @Test
    void userCanForceAProfile() {
        DataProfile forced = registry.resolve("gpu-farm", List.of("GpuUtil", "LatencyMs"), "weather");
        assertEquals("weather", forced.id());
    }

    @Test
    void unknownForcedProfileIsRejectedWithTheList() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> registry.resolve("x", List.of(), "δεν-υπάρχει"));
        assertTrue(e.getMessage().contains("ai-telemetry"), "το μήνυμα πρέπει να λέει τι υπάρχει");
    }

    @Test
    void everyProfileDescribesWhatItDoesDifferently() {
        for (DataProfile profile : registry.all()) {
            assertFalse(profile.id().isBlank());
            assertFalse(profile.title().isBlank());
            assertFalse(profile.approach().isBlank(), profile.id() + " πρέπει να εξηγεί τη μέθοδό του");
        }
    }
}
