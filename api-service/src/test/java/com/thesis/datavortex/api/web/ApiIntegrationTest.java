package com.thesis.datavortex.api.web;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Το API πάνω σε <b>πραγματική TimescaleDB</b>, με το ίδιο schema της παραγωγής.
 * <p>
 * Ελέγχει ό,τι ζει στη βάση και όχι στη Java: ότι τα συνεχή συγκεντρωτικά δίνουν σωστά
 * στατιστικά από sum/sum_sq, ότι το keyset περπατά τις σελίδες χωρίς κενά ή επαναλήψεις, και
 * ότι όλα δουλεύουν πάνω σε σύνδεση <b>read-only</b> — όπως τρέχει στην παραγωγή.
 */
@SpringBootTest
@Testcontainers
class ApiIntegrationTest {

    @Container
    static final PostgreSQLContainer DB = new PostgreSQLContainer(
            DockerImageName.parse("timescale/timescaledb:2.30.2-pg17")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("datavortex_api_it");

    /** Ξεχωριστή σύνδεση με δικαίωμα εγγραφής: το ίδιο το service είναι read-only. */
    private static JdbcTemplate admin;

    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        // Ίδιες παράμετροι με την παραγωγή, ώστε να ελέγχεται και η προστασία read-only
        // Το getJdbcUrl() του Testcontainers μπορεί να φέρει ήδη παραμέτρους: δεύτερο "?" θα
        // αγνοούσε σιωπηλά τη δική μας και το test θα "περνούσε" για λάθος λόγο.
        registry.add("spring.datasource.url", () -> DB.getJdbcUrl()
                + (DB.getJdbcUrl().contains("?") ? "&" : "?")
                + "options=-c%20default_transaction_read_only%3Don");
        registry.add("spring.datasource.username", DB::getUsername);
        registry.add("spring.datasource.password", DB::getPassword);
    }

    @BeforeAll
    static void prepareData() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                DB.getJdbcUrl(), DB.getUsername(), DB.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        admin = new JdbcTemplate(dataSource);

        // Μία πηγή, δύο λεπτά δεδομένων με ΓΝΩΣΤΕΣ τιμές, ώστε τα στατιστικά να ελέγχονται ακριβώς:
        // λεπτό 0 -> 1..10 (μέσος 5.5, τυπική απόκλιση 3.0277), λεπτό 1 -> σταθερά 100.
        for (int i = 1; i <= 10; i++) {
            insert("2026-10-14T10:00:" + String.format("%02d", i) + "Z", i, "Value", i);
        }
        for (int i = 1; i <= 5; i++) {
            insert("2026-10-14T10:01:" + String.format("%02d", i) + "Z", 100 + i, "Value", 100);
        }
        // Δεύτερη πηγή, για να ελεγχθεί ότι τα φίλτρα δουλεύουν.
        insert("2026-10-14T10:00:30Z", 500, "Other", 7, "other-src");
    }

    private static void insert(String time, long offset, String field, double value) {
        insert(time, offset, field, value, "it-src");
    }

    private static void insert(String time, long offset, String field, double value, String source) {
        // Γράφουμε με το ΙΔΙΟ statement που χρησιμοποιεί το ingestion-service, ώστε τα
        // metric_points και το field_catalog να γεμίσουν όπως στην παραγωγή.
        admin.update("""
                WITH ins AS (
                    INSERT INTO ingested_records
                        (event_time, topic, kafka_partition, kafka_offset, source_id, fields, payload)
                    VALUES (?::timestamptz, 'ingest.it', 0, ?, ?, ?::jsonb, '{}'::jsonb)
                    ON CONFLICT DO NOTHING
                    RETURNING event_time, source_id, topic, fields
                ), mp AS (
                    INSERT INTO metric_points (event_time, source_id, topic, field, value)
                    SELECT i.event_time, i.source_id, i.topic, f.key, (f.value #>> '{}')::double precision
                    FROM ins i, jsonb_each(i.fields) f WHERE jsonb_typeof(f.value) = 'number'
                    RETURNING 1
                )
                INSERT INTO field_catalog (source_id, topic, field, json_type, n, first_seen, last_seen)
                SELECT i.source_id, i.topic, f.key, jsonb_typeof(f.value), count(*),
                       min(i.event_time), max(i.event_time)
                FROM ins i, jsonb_each(i.fields) f GROUP BY 1, 2, 3, 4
                ON CONFLICT (source_id, topic, field, json_type) DO UPDATE
                    SET n = field_catalog.n + EXCLUDED.n,
                        last_seen = greatest(field_catalog.last_seen, EXCLUDED.last_seen)
                """, time, offset, source, "{\"" + field + "\":" + value + "}");
    }

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void theServiceConnectionIsReallyReadOnly() {
        JdbcTemplate serviceJdbc = context.getBean(JdbcTemplate.class);
        // Η προστασία δεν είναι σύμβαση: η βάση απορρίπτει την εγγραφή.
        assertThrows(Exception.class, () -> serviceJdbc.update(
                "INSERT INTO field_catalog VALUES ('x','y','z','number',1,now(),now())"));
    }

    @Test
    void sourcesComeFromMetadataWithoutScanningRawData() throws Exception {
        mvc.perform(get("/api/v1/sources"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].sourceId", hasItems("it-src", "other-src")))
                .andExpect(jsonPath("$[?(@.sourceId == 'it-src')].numericFields").value(contains(1)));
    }

    @Test
    void fieldsListKnowsTypes() throws Exception {
        mvc.perform(get("/api/v1/sources/it-src/fields"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].field").value("Value"))
                .andExpect(jsonPath("$[0].jsonType").value("number"))
                .andExpect(jsonPath("$[0].n").value(15));
    }

    @Test
    void seriesComputesStatisticsFromSumsNotFromStoredAverages() throws Exception {
        mvc.perform(get("/api/v1/series")
                        .param("source", "it-src").param("field", "Value")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                        .param("bucket", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bucket").value("1m"))
                .andExpect(jsonPath("$.points", hasSize(2)))
                // 1..10: μέσος 5.5, min 1, max 10, δειγματική τυπική απόκλιση 3.02765
                .andExpect(jsonPath("$.points[0].n").value(10))
                .andExpect(jsonPath("$.points[0].avg").value(5.5))
                .andExpect(jsonPath("$.points[0].min").value(1.0))
                .andExpect(jsonPath("$.points[0].max").value(10.0))
                .andExpect(jsonPath("$.points[0].stddev", closeTo(3.02765, 1e-4)))
                // Σταθερές τιμές: μηδενική διασπορά, χωρίς αρνητικά από στρογγυλοποίηση
                .andExpect(jsonPath("$.points[1].avg").value(100.0))
                .andExpect(jsonPath("$.points[1].stddev").value(0.0));
    }

    @Test
    void hourlyRollupAggregatesTheMinutesCorrectly() throws Exception {
        // 10 τιμές 1..10 (άθροισμα 55) και 5 τιμές 100 (άθροισμα 500) -> 15 τιμές, μέσος 37.
        mvc.perform(get("/api/v1/series")
                        .param("source", "it-src").param("field", "Value")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T12:00:00Z")
                        .param("bucket", "1h"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.points", hasSize(1)))
                .andExpect(jsonPath("$.points[0].n").value(15))
                .andExpect(jsonPath("$.points[0].avg").value(37.0))
                .andExpect(jsonPath("$.points[0].min").value(1.0))
                .andExpect(jsonPath("$.points[0].max").value(100.0));
    }

    @Test
    void matrixReturnsAveragesOrPeaksOnDemand() throws Exception {
        mvc.perform(get("/api/v1/matrix").param("source", "it-src")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                        .param("bucket", "1m").param("agg", "avg"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fields").value(contains("Value")))
                .andExpect(jsonPath("$.rows[0].values.Value").value(5.5));

        mvc.perform(get("/api/v1/matrix").param("source", "it-src")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                        .param("bucket", "1m").param("agg", "max"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rows[0].values.Value").value(10.0));
    }

    @Test
    void unknownAggregationIsRejected() throws Exception {
        mvc.perform(get("/api/v1/matrix").param("source", "it-src").param("agg", "ό,τι να ναι"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void keysetPaginationWalksEveryRowExactlyOnce() throws Exception {
        Set<String> seen = new HashSet<>();
        List<Integer> pageSizes = new ArrayList<>();
        String cursor = null;

        for (int page = 0; page < 10; page++) {
            var request = get("/api/v1/records").param("source", "it-src")
                    .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                    .param("limit", "4");
            if (cursor != null) {
                request = request.param("cursor", cursor);
            }
            String body = mvc.perform(request).andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();

            var json = com.jayway.jsonpath.JsonPath.parse(body);
            List<Object> offsets = json.read("$.records[*].offset");
            pageSizes.add(offsets.size());
            for (Object offset : offsets) {
                assertTrue(seen.add(String.valueOf(offset)), "η γραμμή " + offset + " ήρθε δύο φορές");
            }
            if (!Boolean.TRUE.equals(json.read("$.hasMore"))) {
                break;
            }
            cursor = json.read("$.nextCursor");
        }

        assertEquals(15, seen.size(), "όλες οι γραμμές της πηγής, καμία παραπάνω: " + pageSizes);
        assertTrue(pageSizes.size() >= 4, "πρέπει να χρειάστηκαν πολλές σελίδες: " + pageSizes);
    }

    @Test
    void recordsAreNewestFirstAndCarryTheirFields() throws Exception {
        mvc.perform(get("/api/v1/records").param("source", "it-src")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                        .param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.records[0].eventTime").value("2026-10-14T10:01:05Z"))
                .andExpect(jsonPath("$.records[0].fields.Value").value(100))
                .andExpect(jsonPath("$.records[0].topic").value("ingest.it"))
                .andExpect(jsonPath("$.hasMore").value(true));
    }

    @Test
    void ratesCountRecordsPerBucket() throws Exception {
        mvc.perform(get("/api/v1/rates").param("source", "it-src")
                        .param("from", "2026-10-14T09:00:00Z").param("to", "2026-10-14T11:00:00Z")
                        .param("bucket", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.points", hasSize(2)))
                .andExpect(jsonPath("$.points[0].records").value(10))
                .andExpect(jsonPath("$.points[1].records").value(5));
    }

    @Test
    void anImpossibleRangeIsAClientError() throws Exception {
        mvc.perform(get("/api/v1/series").param("source", "it-src").param("field", "Value")
                        .param("from", "2026-10-14T11:00:00Z").param("to", "2026-10-14T10:00:00Z"))
                .andExpect(status().isBadRequest());
    }
}
