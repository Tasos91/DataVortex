package com.thesis.kafkacollector.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ο writer πάνω σε <b>πραγματική TimescaleDB</b>, με το ίδιο schema που φτιάχνει το Flyway σε
 * παραγωγή (τα migrations έρχονται από το common).
 * <p>
 * Αυτό δεν μπορεί να ελεγχθεί με unit test: το ένα statement γράφει σε τρεις πίνακες με
 * data-modifying CTEs, η idempotency βασίζεται στο ON CONFLICT του Postgres, και τα metric_points
 * παράγονται από το RETURNING — όλα συμπεριφορές της βάσης, όχι της Java.
 */
@Testcontainers
class IngestedRecordWriterIT {

    @Container
    static final PostgreSQLContainer DB = new PostgreSQLContainer(
            DockerImageName.parse("timescale/timescaledb:2.30.2-pg17")
                    .asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("datavortex_it");

    private static JdbcTemplate jdbc;
    private static IngestedRecordWriter writer;
    private static AggregateRefresher refresher;

    @BeforeAll
    static void migrateAndWire() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                // Ίδια παράμετρος με την παραγωγή: ο driver ενώνει τα batched inserts
                DB.getJdbcUrl() + "?reWriteBatchedInserts=true", DB.getUsername(), DB.getPassword());
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();
        jdbc = new JdbcTemplate(dataSource);
        refresher = new AggregateRefresher(jdbc, new SimpleMeterRegistry());
        // Τα @Value δεν εφαρμόζονται χωρίς Spring context: τα ορίζουμε όπως στην παραγωγή.
        org.springframework.test.util.ReflectionTestUtils.setField(refresher, "backfillThresholdMs", 7_200_000L);
        org.springframework.test.util.ReflectionTestUtils.setField(refresher, "enabled", true);
        org.springframework.test.util.ReflectionTestUtils.setField(refresher, "quietMs", 0L);
        writer = new IngestedRecordWriter(jdbc, refresher, new SimpleMeterRegistry());
    }

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE ingested_records, metric_points, field_catalog");
        // Ο refresher είναι κοινός για όλη την κλάση και ο helper row() γράφει με χρόνο αρκετά
        // παλιό ώστε να θεωρείται backfill: χωρίς αυτό, κάθε test κληρονομεί το εύρος του προηγούμενου.
        refresher.discardPending();
    }

    private static IngestedRecordWriter.Row row(long offset, String fields) {
        return new IngestedRecordWriter.Row(
                Instant.parse("2026-10-14T10:00:00Z").toEpochMilli() + offset * 1000,
                "ingest.it", 0, offset, "it-src", fields, "{\"raw\":true}");
    }

    @Test
    void oneStatementFillsAllThreeTables() {
        int written = writer.write(List.of(
                row(1, "{\"Speed\":10.5,\"State\":\"ok\"}"),
                row(2, "{\"Speed\":20.5,\"State\":\"warn\"}")));

        assertEquals(2, written);
        assertEquals(2, count("ingested_records"));
        // Μόνο τα ΑΡΙΘΜΗΤΙΚΑ πεδία γίνονται metric_points· το κειμενικό State δεν είναι μετρική.
        assertEquals(2, count("metric_points"));
        assertEquals(List.of("Speed"), jdbc.queryForList(
                "SELECT DISTINCT field FROM metric_points", String.class));
        // Το field_catalog ξέρει και για τα κειμενικά, ώστε το UI να μη σαρώνει τη βάση.
        assertEquals(2, count("field_catalog"));
        assertEquals("string", jdbc.queryForObject(
                "SELECT json_type FROM field_catalog WHERE field = 'State'", String.class));
    }

    @Test
    void duplicatesAreRejectedAndDoNotDoubleCountMetrics() {
        writer.write(List.of(row(1, "{\"Speed\":10.0}")));
        int again = writer.write(List.of(row(1, "{\"Speed\":99.0}")));

        assertEquals(0, again, "ίδιο (topic, partition, offset): δεν γράφεται ξανά");
        assertEquals(1, count("ingested_records"));
        // Το κρίσιμο: τα metric_points παράγονται από το RETURNING, άρα το διπλό δεν έφτιαξε
        // δεύτερη μετρική — αλλιώς κάθε rebalance του Kafka θα διόγκωνε τα στατιστικά.
        assertEquals(1, count("metric_points"));
        assertEquals(10.0, jdbc.queryForObject("SELECT value FROM metric_points", Double.class));
        // Ο κατάλογος μετρά μόνο ό,τι μπήκε όντως.
        assertEquals(1L, jdbc.queryForObject("SELECT n FROM field_catalog WHERE field='Speed'", Long.class));
    }

    @Test
    void catalogAccumulatesCountsAndTimestampsAcrossBatches() {
        writer.write(List.of(row(1, "{\"Speed\":1.0}"), row(2, "{\"Speed\":2.0}")));
        writer.write(List.of(row(3, "{\"Speed\":3.0}")));

        Map<String, Object> catalog = jdbc.queryForMap("SELECT * FROM field_catalog WHERE field='Speed'");
        assertEquals(3L, catalog.get("n"));
        assertTrue(((java.sql.Timestamp) catalog.get("first_seen"))
                .before((java.sql.Timestamp) catalog.get("last_seen")));
    }

    @Test
    void continuousAggregateSeesTheDataImmediately() {
        List<IngestedRecordWriter.Row> rows = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            rows.add(row(i, "{\"Speed\":" + i + "}"));
        }
        assertEquals(10, writer.write(rows));

        // materialized_only = false: τα φρέσκα δεδομένα φαίνονται χωρίς να τρέξει κανένα refresh.
        Map<String, Object> bucket = jdbc.queryForMap("""
                SELECT sum(n) AS n, sum(sum_v) AS sum_v, min(min_v) AS min_v, max(max_v) AS max_v
                FROM metric_1m WHERE source_id = 'it-src' AND field = 'Speed'""");
        assertEquals(10L, ((Number) bucket.get("n")).longValue());
        assertEquals(55.0, (Double) bucket.get("sum_v"), 1e-9);
        assertEquals(1.0, (Double) bucket.get("min_v"), 1e-9);
        assertEquals(10.0, (Double) bucket.get("max_v"), 1e-9);
    }

    @Test
    void aBadRowDoesNotTakeTheWholeBatchDownWithIt() {
        // Το payload δεν είναι έγκυρο jsonb: η γραμμή πρέπει να απορριφθεί μόνη της.
        IngestedRecordWriter.Row broken = new IngestedRecordWriter.Row(
                Instant.parse("2026-10-14T10:00:05Z").toEpochMilli(),
                "ingest.it", 0, 99, "it-src", "{\"Speed\":1.0}", "ΔΕΝ ΕΙΝΑΙ JSON");

        int written = writer.write(List.of(row(1, "{\"Speed\":1.0}"), broken, row(2, "{\"Speed\":2.0}")));

        assertEquals(2, written, "οι δύο καλές γραμμές πρέπει να περάσουν");
        assertEquals(2, count("ingested_records"));
        assertEquals(1, writer.getTotalSkipped());
    }

    @Test
    void largeBatchIsSplitAndStillLandsCompletely() {
        List<IngestedRecordWriter.Row> rows = new ArrayList<>();
        for (int i = 1; i <= 2500; i++) {
            rows.add(row(i, "{\"Speed\":" + (i % 50) + ",\"Other\":" + i + "}"));
        }
        assertEquals(2500, writer.write(rows));
        assertEquals(2500, count("ingested_records"));
        assertEquals(5000, count("metric_points"), "δύο αριθμητικά πεδία ανά γραμμή");
    }

    @Test
    void historicalDataIsInvisibleUntilTheAggregatesAreRefreshed() {
        // Το bug: οι πολιτικές refresh καλύπτουν ένα παράθυρο γύρω από το τώρα (3 ώρες για το 1m).
        // Ένα CSV με δεδομένα του περασμένου μήνα έμπαινε στη βάση και τα ερωτήματα γύρναγαν ΑΔΕΙΑ,
        // χωρίς κανένα σφάλμα πουθενά — η χειρότερη κατηγορία σφάλματος.
        // Πρώτα ανεβάζουμε το watermark με ένα refresh του «τώρα», όπως κάνει η πολιτική σε
        // παραγωγή. Χωρίς αυτό το βήμα το watermark είναι -infinity, το real-time σκέλος δείχνει
        // τα πάντα, και το bug δεν φαίνεται καθόλου στο test.
        long now = System.currentTimeMillis();
        writer.write(List.of(new IngestedRecordWriter.Row(now, "ingest.it", 0, 1,
                "warm-src", "{\"Speed\":1.0}", null)));
        long day = 86_400_000L;
        jdbc.update("CALL refresh_continuous_aggregate('metric_1m', ?::timestamptz, ?::timestamptz)",
                new java.sql.Timestamp(Math.floorDiv(now, day) * day),
                new java.sql.Timestamp((Math.floorDiv(now, day) + 1) * day));

        long old = Instant.parse("2026-08-01T10:00:00Z").toEpochMilli();
        List<IngestedRecordWriter.Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(new IngestedRecordWriter.Row(old + i * 60_000L, "ingest.it", 0, 1000 + i,
                    "old-src", "{\"Speed\":" + (i + 1) + "}", null));
        }
        assertEquals(10, writer.write(rows));
        assertEquals(11, count("metric_points"), "τα ωμά σημεία είναι εκεί (10 παλιά + 1 φρέσκο)");

        assertTrue(refresher.hasPending(), "ο writer πρέπει να έχει δηλώσει το παλιό εύρος");
        assertEquals(0, materialized1m("old-src"),
                "πριν το refresh το συγκεντρωτικό είναι άδειο — αυτό ήταν το αόρατο bug");

        refresher.refreshIfDirty();

        assertEquals(10L, materialized1m("old-src"), "μετά το refresh τα δεδομένα υπάρχουν");
        assertFalse(refresher.hasPending(), "το εύρος καταναλώθηκε");
    }

    @Test
    void liveDataNeedsNoRefreshAtAll() {
        // Φρέσκα δεδομένα: η πολιτική και το real-time aggregation τα καλύπτουν, άρα δεν έχει
        // νόημα να πληρώνουμε ένα refresh ανά batch.
        writer.write(List.of(new IngestedRecordWriter.Row(System.currentTimeMillis(),
                "ingest.it", 0, 1, "live-src", "{\"Speed\":1.0}", null)));

        assertFalse(refresher.hasPending());
    }

    /** Μόνο το υλοποιημένο κομμάτι του CAGG, χωρίς το real-time σκέλος. */
    private static long materialized1m(String source) {
        Long n = jdbc.queryForObject("""
                SELECT coalesce(sum(n), 0) FROM metric_1m
                WHERE source_id = ? AND field = 'Speed'""", Long.class, source);
        return n == null ? 0 : n;
    }

    private static int count(String table) {
        return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class);
    }
}
