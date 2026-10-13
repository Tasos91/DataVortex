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
