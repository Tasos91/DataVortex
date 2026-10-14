package com.thesis.kafkacollector.service;

import java.util.Map;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Γράφει στο TimescaleDB ό,τι διάβασε ο consumer από το Kafka.
 * <p>
 * Ένα INSERT με πολλές γραμμές ανά batch, αντί για ένα INSERT ανά record: η διαφορά είναι
 * ένα round-trip στη βάση αντί για N. Το {@code ON CONFLICT DO NOTHING} πάνω στο
 * {@code (event_time, topic, kafka_partition, kafka_offset)} κάνει την εγγραφή idempotent,
 * που είναι απαραίτητο αφού ο consumer είναι at-least-once.
 */
@Service
public class IngestedRecordWriter {

    private static final Logger log = LoggerFactory.getLogger(IngestedRecordWriter.class);

    /** event_time, topic, kafka_partition, kafka_offset, source_id, fields, payload */
    private static final int COLUMNS = 7;
    /**
     * Ένα statement γράφει σε ΤΡΕΙΣ πίνακες, με data-modifying CTEs:
     * <ol>
     *   <li>{@code ingested_records} — η πηγή αλήθειας, idempotent με ON CONFLICT DO NOTHING·</li>
     *   <li>{@code metric_points} — μία γραμμή ανά αριθμητικό πεδίο, από το {@code RETURNING} του
     *       πρώτου, άρα μόνο για όσα μπήκαν πραγματικά (τα διπλά δεν παράγουν μετρικές)·</li>
     *   <li>{@code field_catalog} — ποια πεδία υπάρχουν ανά πηγή, για να μη σαρώνει το UI τη βάση.</li>
     * </ol>
     * Το τελικό {@code SELECT count(*) FROM ins} δίνει πόσες γραμμές μπήκαν· τα data-modifying
     * CTEs εκτελούνται πάντα, ανεξάρτητα από το αν το κύριο query διαβάζει το αποτέλεσμά τους.
     * Ένα round-trip για τα τρία.
     */
    private static final String INSERT_PREFIX = """
            WITH ins AS (
                INSERT INTO ingested_records
                    (event_time, topic, kafka_partition, kafka_offset, source_id, fields, payload)
                VALUES """;
    private static final String VALUES_TUPLE = "(?,?,?,?,?,?::jsonb,?::jsonb)";
    private static final String INSERT_SUFFIX = """
            \n    ON CONFLICT DO NOTHING
                RETURNING event_time, source_id, topic, fields
            ), kv AS (
                -- Το jsonb ανοίγεται ΜΙΑ φορά και το αποτέλεσμα το μοιράζονται και οι δύο
                -- επόμενοι πίνακες. Με δύο ξεχωριστά jsonb_each η ανάλυση του jsonb γινόταν
                -- διπλή, και είναι το ακριβότερο κομμάτι της εγγραφής.
                SELECT i.event_time, i.source_id, i.topic,
                       f.key AS field, f.value AS value, jsonb_typeof(f.value) AS json_type
                FROM ins i, jsonb_each(i.fields) f
            ), mp AS (
                INSERT INTO metric_points (event_time, source_id, topic, field, value)
                SELECT event_time, source_id, topic, field, (value #>> '{}')::double precision
                FROM kv WHERE json_type = 'number'
                RETURNING 1
            ), cat AS (
                INSERT INTO field_catalog (source_id, topic, field, json_type, n, first_seen, last_seen)
                SELECT source_id, topic, field, json_type, count(*), min(event_time), max(event_time)
                FROM kv
                GROUP BY 1, 2, 3, 4
                -- ΣΤΑΘΕΡΗ ΣΕΙΡΑ, όχι διακόσμηση: χωρίς αυτήν, παράλληλοι consumers κλειδώνουν
                -- τις ίδιες γραμμές του καταλόγου με διαφορετική σειρά και η Postgres ανιχνεύει
                -- deadlock. Φάνηκε μόνο υπό φορτίο, με 6 threads και 100.000 εγγραφές.
                ORDER BY 1, 2, 3, 4
                ON CONFLICT (source_id, topic, field, json_type) DO UPDATE
                    SET n = field_catalog.n + EXCLUDED.n,
                        first_seen = least(field_catalog.first_seen, EXCLUDED.first_seen),
                        last_seen = greatest(field_catalog.last_seen, EXCLUDED.last_seen)
                RETURNING 1
            )
            SELECT count(*) FROM ins""";

    private static final int DEFAULT_BATCH_ROWS = 1000;

    private final JdbcTemplate jdbc;
    private final AggregateRefresher refresher;
    private final Timer insertTimer;
    private final Counter rowsWritten;
    private final Counter rowsDuplicate;
    private final Counter rowsRejected;
    /** Το SQL ανά μέγεθος batch· φτιάχνεται μία φορά και ξαναχρησιμοποιείται. */
    private final ConcurrentHashMap<Integer, String> sqlCache = new ConcurrentHashMap<>();
    private final AtomicLong totalWritten = new AtomicLong();
    private final AtomicLong totalSkipped = new AtomicLong();

    @Value("${datavortex.batch-rows:1000}")
    private int batchRows;

    public IngestedRecordWriter(JdbcTemplate jdbc, AggregateRefresher refresher, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.refresher = refresher;
        this.insertTimer = Timer.builder("datavortex.db.insert")
                .description("Διάρκεια ενός multi-row INSERT")
                .publishPercentileHistogram()
                .register(registry);
        this.rowsWritten = rows(registry, "written");
        this.rowsDuplicate = rows(registry, "duplicate");
        this.rowsRejected = rows(registry, "rejected");
    }

    private static Counter rows(MeterRegistry registry, String result) {
        return Counter.builder("datavortex.db.rows")
                .description("Γραμμές που πήγαν στη βάση, ανά αποτέλεσμα")
                .tag("result", result)
                .register(registry);
    }

    /**
     * Μία γραμμή του πίνακα. Το {@code eventTime} είναι το timestamp του μηνύματος στο Kafka
     * (σταθερό σε κάθε επανάληψη), ώστε το dedup να δουλεύει.
     */
    public record Row(long eventTimeMillis,
                      String topic,
                      int partition,
                      long offset,
                      String sourceId,
                      String fieldsJson,
                      String payloadJson) {
    }

    /**
     * @return πόσες γραμμές γράφτηκαν πραγματικά (τα διπλά δεν μετρούν)
     */
    public int write(List<Row> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        int written = 0;
        long started = System.nanoTime();
        // Φρουρός: με batchRows <= 0 (π.χ. αν λείψει η ρύθμιση) το βήμα θα ήταν μηδέν και ο
        // βρόχος θα έτρεχε για πάντα.
        int step = batchRows > 0 ? batchRows : DEFAULT_BATCH_ROWS;
        for (int start = 0; start < rows.size(); start += step) {
            List<Row> chunk = rows.subList(start, Math.min(start + step, rows.size()));
            written += writeChunk(chunk);
        }
        insertTimer.record(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS);

        if (written > 0) {
            // Αν το batch ήταν ιστορικό (backfill), τα συγκεντρωτικά δεν θα το δουν ποτέ μόνα τους.
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (Row row : rows) {
                min = Math.min(min, row.eventTimeMillis());
                max = Math.max(max, row.eventTimeMillis());
            }
            refresher.noteWritten(min, max);
        }
        totalWritten.addAndGet(written);
        rowsWritten.increment(written);
        // Ό,τι δεν γράφτηκε και δεν απορρίφθηκε, ήταν διπλό (ON CONFLICT DO NOTHING)
        rowsDuplicate.increment(Math.max(0, rows.size() - written));
        return written;
    }

    private int writeChunk(List<Row> chunk) {
        String sql = sqlCache.computeIfAbsent(chunk.size(), IngestedRecordWriter::buildSql);
        Object[] args = new Object[chunk.size() * COLUMNS];
        int i = 0;
        for (Row row : chunk) {
            i = fill(args, i, row);
        }
        try {
            Integer inserted = jdbc.queryForObject(sql, Integer.class, args);
            return inserted != null ? inserted : 0;
        } catch (DataAccessException e) {
            // Μπορεί να φταίει μία μόνο γραμμή (π.χ. άκυρο JSON). Ξαναδοκιμάζουμε μία-μία
            // ώστε να περάσουν οι υπόλοιπες, αντί να χαθεί όλο το batch.
            // Αν φταίει η σύνδεση, θα ξαναπεταχτεί και θα το χειριστεί ο error handler του Kafka.
            if (chunk.size() == 1) {
                throw e;
            }
            log.warn("Το batch των {} γραμμών απέτυχε ({}). Δοκιμή μία-μία.", chunk.size(), e.getMessage());
            return writeOneByOne(chunk);
        }
    }

    private int writeOneByOne(List<Row> chunk) {
        String sql = sqlCache.computeIfAbsent(1, IngestedRecordWriter::buildSql);
        int written = 0;
        RuntimeException connectionProblem = null;
        for (Row row : chunk) {
            Object[] args = new Object[COLUMNS];
            fill(args, 0, row);
            try {
                Integer inserted = jdbc.queryForObject(sql, Integer.class, args);
                written += inserted != null ? inserted : 0;
            } catch (DataAccessException e) {
                if (e instanceof org.springframework.dao.DataAccessResourceFailureException
                        || e instanceof org.springframework.dao.TransientDataAccessException) {
                    connectionProblem = e; // πρόβλημα βάσης, όχι δεδομένων: πρέπει να ξαναδοκιμαστεί
                    break;
                }
                totalSkipped.incrementAndGet();
                rowsRejected.increment();
                log.error("Η γραμμή {}-{} offset {} απορρίφθηκε από τη βάση: {}",
                        row.topic(), row.partition(), row.offset(), e.getMessage());
            }
        }
        if (connectionProblem != null) {
            throw connectionProblem;
        }
        return written;
    }

    private static int fill(Object[] args, int i, Row row) {
        args[i++] = new Timestamp(row.eventTimeMillis());
        args[i++] = row.topic();
        args[i++] = row.partition();
        args[i++] = row.offset();
        args[i++] = row.sourceId();
        args[i++] = row.fieldsJson();
        args[i++] = row.payloadJson();
        return i;
    }

    private static String buildSql(int rows) {
        StringBuilder sb = new StringBuilder(
                INSERT_PREFIX.length() + INSERT_SUFFIX.length() + rows * (VALUES_TUPLE.length() + 1));
        sb.append(INSERT_PREFIX);
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(VALUES_TUPLE);
        }
        return sb.append(INSERT_SUFFIX).toString();
    }

    /**
     * Τα δυναμικά πεδία του proto ως JSON, για τη στήλη jsonb.
     * <p>
     * Γράφεται με το χέρι και όχι με το {@code JsonFormat} του protobuf-java-util, που βγάζει
     * <b>213.447 εγγραφές/s</b>: στήνει {@code Struct}, περνά από αντανάκλαση και μητρώο τύπων, και
     * παράγει κείμενο μέσω ενδιάμεσων αντικειμένων. Εδώ ξέρουμε ακριβώς τι έχουμε — ένα επίπεδο
     * map από όνομα σε τιμή — και το γράφουμε κατευθείαν σε έναν {@code StringBuilder}.
     * <p>
     * Η Postgres κανονικοποιεί ούτως ή άλλως το jsonb (σειρά κλειδιών, μορφή αριθμών), άρα αυτό που
     * χρειάζεται είναι <b>σημασιολογική</b> ισοδυναμία, όχι ίδια bytes. Υπάρχει test που συγκρίνει
     * τις δύο διαδρομές πεδίο προς πεδίο.
     */
    public static String fieldsToJson(IngestedRecordProto.IngestedRecord record) {
        if (record.getFieldsCount() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder(32 + record.getFieldsCount() * 24);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, com.google.protobuf.Value> entry : record.getFieldsMap().entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            escape(sb, entry.getKey());
            sb.append(':');
            writeValue(sb, entry.getValue());
        }
        return sb.append('}').toString();
    }

    private static void writeValue(StringBuilder sb, com.google.protobuf.Value value) {
        switch (value.getKindCase()) {
            case NUMBER_VALUE -> writeNumber(sb, value.getNumberValue());
            case STRING_VALUE -> escape(sb, value.getStringValue());
            case BOOL_VALUE -> sb.append(value.getBoolValue() ? "true" : "false");
            case LIST_VALUE -> {
                sb.append('[');
                List<com.google.protobuf.Value> values = value.getListValue().getValuesList();
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    writeValue(sb, values.get(i));
                }
                sb.append(']');
            }
            case STRUCT_VALUE -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<String, com.google.protobuf.Value> e : value.getStructValue().getFieldsMap().entrySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    escape(sb, e.getKey());
                    sb.append(':');
                    writeValue(sb, e.getValue());
                }
                sb.append('}');
            }
            default -> sb.append("null"); // NULL_VALUE και KIND_NOT_SET
        }
    }

    /**
     * Ακέραιοι χωρίς «.0», και — κυρίως — το NaN και το άπειρο γίνονται {@code null}: <b>δεν είναι
     * έγκυρο JSON</b>, και θα έκαναν τη βάση να απορρίψει ολόκληρη τη γραμμή.
     */
    private static void writeNumber(StringBuilder sb, double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            sb.append("null");
        } else if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            sb.append((long) d);
        } else {
            sb.append(d);
        }
    }

    private static void escape(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    public long getTotalWritten() {
        return totalWritten.get();
    }

    public long getTotalSkipped() {
        return totalSkipped.get();
    }
}
