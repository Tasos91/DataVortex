package com.thesis.datavortex.analytics.web;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Service-to-service: το analytics-ui μιλά στο api-service μέσω <b>πραγματικού HTTP</b>.
 * <p>
 * Στη θέση του api-service μπαίνει ένας μικροσκοπικός server που επιστρέφει γνωστά δεδομένα.
 * Έτσι ελέγχεται ό,τι σπάει στην πράξη μεταξύ υπηρεσιών και δεν φαίνεται σε unit test: τα
 * query params, η αποσειριοποίηση του JSON (συμπεριλαμβανομένων των Instant), η επιλογή
 * προφίλ από τα ονόματα των πεδίων, και ότι ζητούνται ΚΑΙ οι μέσοι όροι ΚΑΙ οι αιχμές.
 */
@SpringBootTest
class AnalyticsHttpIntegrationTest {

    private static HttpServer stub;
    private static final AtomicInteger matrixCalls = new AtomicInteger();
    private static final List<String> seenQueries = new ArrayList<>();

    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    @BeforeAll
    static void startStubApi() throws IOException {
        stub = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

        stub.createContext("/api/v1/matrix", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            seenQueries.add(query);
            matrixCalls.incrementAndGet();
            boolean peaks = query.contains("agg=max");
            respond(exchange, matrixJson(peaks));
        });
        stub.createContext("/api/v1/sources", exchange -> respond(exchange, """
                [{"sourceId":"gpu-farm","topics":["ingest.ai"],"records":1200,
                  "firstSeen":"2026-10-14T00:00:00Z","lastSeen":"2026-10-14T01:00:00Z",
                  "numericFields":2,"textFields":0}]"""));
        stub.start();
    }

    @AfterAll
    static void stopStubApi() {
        stub.stop(0);
    }

    @DynamicPropertySource
    static void pointAtStub(DynamicPropertyRegistry registry) {
        registry.add("datavortex.api-base-url", () -> "http://127.0.0.1:" + stub.getAddress().getPort());
    }

    /** Δύο καθαρά καθεστώτα φορτίου, με μία κρυφή αιχμή καθυστέρησης μόνο στα max. */
    private static String matrixJson(boolean peaks) {
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < 40; i++) {
            boolean busy = i >= 20;
            double util = busy ? 92 + i % 3 : 9 + i % 3;
            double latency = busy ? 180 + i % 4 : 14 + i % 2;
            if (peaks) {
                util += 3;
                latency += 5;
                if (i == 7) {
                    latency = 4000; // αιχμή που ο μέσος όρος του κουβά δεν δείχνει ποτέ
                }
            }
            rows.append(i > 0 ? "," : "")
                    .append("{\"t\":\"2026-10-14T00:%02d:00Z\",\"n\":100,\"values\":{\"GpuUtil\":%s,\"LatencyMs\":%s}}"
                            .formatted(i, util, latency));
        }
        return """
                {"source":"gpu-farm","bucket":"1m","from":"2026-10-14T00:00:00Z","to":"2026-10-14T00:40:00Z",
                 "fields":["GpuUtil","LatencyMs"],"rows":[%s]}""".formatted(rows);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        matrixCalls.set(0);
        seenQueries.clear();
    }

    @Test
    void analysisTravelsOverHttpAndPicksTheTelemetryProfile() throws Exception {
        mvc.perform(get("/api/analysis").param("source", "gpu-farm").param("range", "1h").param("bucket", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.profile").value("ai-telemetry"))
                .andExpect(jsonPath("$.points").value(40))
                .andExpect(jsonPath("$.features", containsInAnyOrder("GpuUtil", "LatencyMs")))
                // Δύο καθεστώτα στα δεδομένα, άρα δύο ομάδες με ονόματα φορτίου
                .andExpect(jsonPath("$.clusters", hasSize(2)))
                .andExpect(jsonPath("$.clusters[*].label", hasItems("αδράνεια", "κορεσμός")))
                .andExpect(jsonPath("$.verdict.status", not(emptyString())));
    }

    @Test
    void bothAveragesAndPeaksAreRequested() throws Exception {
        mvc.perform(get("/api/analysis").param("source", "gpu-farm").param("range", "1h").param("bucket", "1m"))
                .andExpect(status().isOk());

        assertTrue(seenQueries.stream().anyMatch(q -> q.contains("agg=avg")), "λείπουν οι μέσοι όροι: " + seenQueries);
        assertTrue(seenQueries.stream().anyMatch(q -> q.contains("agg=max")), "λείπουν οι αιχμές: " + seenQueries);
        assertTrue(seenQueries.stream().allMatch(q -> q.contains("source=gpu-farm")));
    }

    @Test
    void theHiddenSpikeIsFoundOnlyBecausePeaksAreFetched() throws Exception {
        mvc.perform(get("/api/analysis").param("source", "gpu-farm").param("range", "1h").param("bucket", "1m"))
                .andExpect(status().isOk())
                // 4000ms υπάρχει ΜΟΝΟ στα max· αν το service ζητούσε μόνο μέσους όρους, καμία ανωμαλία
                .andExpect(jsonPath("$.anomalies[?(@.value > 3000)]", hasSize(greaterThan(0))));
    }

    @Test
    void sourcesAreProxiedThroughToTheUi() throws Exception {
        mvc.perform(get("/api/sources"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].sourceId").value("gpu-farm"))
                .andExpect(jsonPath("$[0].records").value(1200))
                .andExpect(jsonPath("$[0].lastSeen").value("2026-10-14T01:00:00Z"));
    }

    @Test
    void profilesEndpointExplainsEachApproach() throws Exception {
        mvc.perform(get("/api/profiles"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].id", hasItems("ai-telemetry", "weather", "generic")))
                .andExpect(jsonPath("$[*].approach", everyItem(not(emptyString()))));
    }

    @Test
    void repeatedRequestIsServedFromCacheWithoutCallingTheApiAgain() throws Exception {
        for (int i = 0; i < 3; i++) {
            mvc.perform(get("/api/analysis").param("source", "gpu-farm")
                            .param("from", "2026-10-14T00:00:00Z").param("to", "2026-10-14T01:00:00Z")
                            .param("bucket", "1m"))
                    .andExpect(status().isOk());
        }
        // Σταθερό διάστημα, άρα σταθερό κλειδί cache: μόνο η πρώτη κλήση φτάνει στο api-service
        // (δύο requests: μέσοι όροι και αιχμές).
        assertTrue(matrixCalls.get() <= 2,
                "το cache δεν δούλεψε, κλήσεις στο api: " + matrixCalls.get());
    }

    @Test
    void badRangeIsRejectedBeforeAnyCallToTheApi() throws Exception {
        mvc.perform(get("/api/analysis").param("source", "gpu-farm").param("range", "πολύ"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error", containsString("range")));
        assertTrue(seenQueries.isEmpty(), "δεν πρέπει να χτυπηθεί το api για άκυρο αίτημα");
    }
}
