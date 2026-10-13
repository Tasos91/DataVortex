package com.thesis.kafkacollector.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class IngestedRecordWriter {

    private static final int COLUMNS = 9;
    private static final String INSERT_PREFIX = """
            INSERT INTO ingested_records
                (event_time, topic, kafka_partition, kafka_offset, source_id, lat, lon, fields, payload)
            VALUES """;
    private static final String VALUES_TUPLE = "(?,?,?,?,?,?,?,?::jsonb,?::jsonb)";
    private static final String ON_CONFLICT = " ON CONFLICT DO NOTHING";

    private static final JsonFormat.Printer JSON = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .includingDefaultValueFields();

    private final JdbcTemplate jdbc;
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

    public IngestedRecordWriter(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
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
                      Double lat,
                      Double lon,
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
        for (int start = 0; start < rows.size(); start += batchRows) {
            List<Row> chunk = rows.subList(start, Math.min(start + batchRows, rows.size()));
            written += writeChunk(chunk);
        }
        insertTimer.record(System.nanoTime() - started, java.util.concurrent.TimeUnit.NANOSECONDS);
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
            return jdbc.update(sql, args);
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
                written += jdbc.update(sql, args);
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
        args[i++] = row.lat();
        args[i++] = row.lon();
        args[i++] = row.fieldsJson();
        args[i++] = row.payloadJson();
        return i;
    }

    private static String buildSql(int rows) {
        StringBuilder sb = new StringBuilder(INSERT_PREFIX.length() + rows * (VALUES_TUPLE.length() + 1) + 32);
        sb.append(INSERT_PREFIX);
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(VALUES_TUPLE);
        }
        return sb.append(ON_CONFLICT).toString();
    }

    /** Τα δυναμικά πεδία του proto ως JSON, για τη στήλη jsonb. */
    public static String fieldsToJson(IngestedRecordProto.IngestedRecord record) {
        if (record.getFieldsCount() == 0) {
            return null;
        }
        try {
            return JSON.print(Struct.newBuilder().putAllFields(record.getFieldsMap()).build());
        } catch (InvalidProtocolBufferException e) {
            log.warn("Τα δυναμικά πεδία δεν έγιναν JSON: {}", e.getMessage());
            return null;
        }
    }

    public long getTotalWritten() {
        return totalWritten.get();
    }

    public long getTotalSkipped() {
        return totalSkipped.get();
    }
}
