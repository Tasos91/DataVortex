package com.thesis.datavortex.api.repo;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import com.thesis.datavortex.api.model.Dtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Το μόνο σημείο όλου του συστήματος που διαβάζει από τη βάση.
 * <p>
 * Κανένα query δεν σαρώνει ωμά δεδομένα για συγκεντρωτικά: όλα διαβάζουν συνεχή
 * συγκεντρωτικά (metric_1m/1h/1d, records_1m). Τα ωμά δεδομένα διαβάζονται μόνο σελίδα-σελίδα
 * με keyset cursor, και πάντα με όριο.
 */
@Repository
public class AnalyticsRepository {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

    /** Σκληρό πλαφόν: κανένα request δεν μπορεί να ζητήσει περισσότερα. */
    public static final int MAX_PAGE = 1000;

    private final JdbcTemplate jdbc;

    public AnalyticsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ---------------------------------------------------------------- μεταδεδομένα

    public List<Dtos.Source> sources() {
        // Από το field_catalog (μικρός πίνακας) και το records_1d του Timescale: κανένα άγγιγμα
        // στα ωμά δεδομένα, άρα ο χρόνος δεν εξαρτάται από τον όγκο.
        return jdbc.query("""
                SELECT c.source_id,
                       array_agg(DISTINCT c.topic)                                    AS topics,
                       count(*) FILTER (WHERE c.json_type = 'number')                 AS numeric_fields,
                       count(*) FILTER (WHERE c.json_type <> 'number')                AS text_fields,
                       min(c.first_seen)                                              AS first_seen,
                       max(c.last_seen)                                               AS last_seen,
                       coalesce((SELECT sum(n) FROM records_1m r WHERE r.source_id = c.source_id), 0) AS records
                FROM field_catalog c
                GROUP BY c.source_id
                ORDER BY max(c.last_seen) DESC
                """, (rs, i) -> new Dtos.Source(
                rs.getString("source_id"),
                List.of((String[]) rs.getArray("topics").getArray()),
                rs.getLong("records"),
                instant(rs.getTimestamp("first_seen")),
                instant(rs.getTimestamp("last_seen")),
                rs.getInt("numeric_fields"),
                rs.getInt("text_fields")));
    }

    public List<Dtos.Field> fields(String source) {
        return jdbc.query("""
                SELECT field, json_type, n, first_seen, last_seen
                FROM field_catalog WHERE source_id = ?
                ORDER BY (json_type = 'number') DESC, field
                """, (rs, i) -> new Dtos.Field(
                rs.getString("field"), rs.getString("json_type"), rs.getLong("n"),
                instant(rs.getTimestamp("first_seen")), instant(rs.getTimestamp("last_seen"))),
                source);
    }

    // ---------------------------------------------------------------- χρονοσειρές

    public List<Dtos.Point> series(String source, String field, Instant from, Instant to, Buckets bucket) {
        // Το view μπαίνει με string concatenation επειδή ο πίνακας δεν μπορεί να είναι
        // παράμετρος· η τιμή προέρχεται ΜΟΝΟ από το enum Buckets, ποτέ από τον χρήστη.
        // Το time_bucket επιτρέπει ενδιάμεσα επίπεδα (5m, 15m, 6h) χωρίς νέα views: όταν το
        // πλάτος ισούται με του view είναι άκοπο no-op, αλλιώς αθροίζει τα sum/sum_sq σωστά.
        String sql = """
                SELECT time_bucket(?::interval, bucket) AS t,
                       sum(n) AS n, sum(sum_v) AS sum_v, sum(sum_sq) AS sum_sq,
                       min(min_v) AS min_v, max(max_v) AS max_v
                FROM %s
                WHERE source_id = ? AND field = ? AND bucket >= ? AND bucket < ?
                GROUP BY t ORDER BY t
                """.formatted(bucket.metricView());
        return jdbc.query(sql, (rs, i) -> point(instant(rs.getTimestamp("t")), rs.getLong("n"),
                        rs.getDouble("sum_v"), rs.getDouble("sum_sq"),
                        rs.getDouble("min_v"), rs.getDouble("max_v")),
                interval(bucket), source, field, Timestamp.from(from), Timestamp.from(to));
    }

    /**
     * Όλα τα αριθμητικά πεδία μιας πηγής σε έναν πίνακα κουβάδων × πεδίων, με ένα query.
     *
     * @param agg τι αντιπροσωπεύει την τιμή του κουβά: {@code avg} για τη συμπεριφορά,
     *            {@code max}/{@code min} για τις αιχμές. Ο μέσος όρος εξαφανίζει τις αιχμές
     *            (μία αργή μέτρηση μέσα σε 80 γρήγορες δεν φαίνεται), άρα η ανίχνευση
     *            ανωμαλιών χρειάζεται {@code max}.
     */
    public Dtos.Matrix matrix(String source, List<String> fields, Instant from, Instant to,
                              Buckets bucket, String agg) {
        String value = switch (agg == null ? "avg" : agg.toLowerCase()) {
            case "max" -> "max(max_v)";
            case "min" -> "min(min_v)";
            case "avg" -> "sum(sum_v) / nullif(sum(n), 0)";
            default -> throw new IllegalArgumentException("Άγνωστο agg '" + agg + "'. Δεκτά: avg, max, min.");
        };
        String sql = """
                SELECT time_bucket(?::interval, bucket) AS t, field, sum(n) AS n, %s AS v
                FROM %s
                WHERE source_id = ? AND bucket >= ? AND bucket < ?
                  AND (?::boolean OR field = ANY (?::text[]))
                GROUP BY t, field ORDER BY t
                """.formatted(value, bucket.metricView());

        boolean allFields = fields == null || fields.isEmpty();
        Map<Instant, Map<String, Double>> values = new LinkedHashMap<>();
        Map<Instant, Long> counts = new LinkedHashMap<>();
        List<String> seen = new ArrayList<>();

        jdbc.query(sql, rs -> {
            Instant t = instant(rs.getTimestamp("t"));
            String field = rs.getString("field");
            long n = rs.getLong("n");
            values.computeIfAbsent(t, key -> new LinkedHashMap<>()).put(field, round(rs.getDouble("v")));
            // Ένα πεδίο μπορεί να λείπει από κάποια μηνύματα· κρατάμε το μεγαλύτερο πλήθος
            // ως "πόσες εγγραφές είχε αυτός ο κουβάς".
            counts.merge(t, n, Math::max);
            if (!seen.contains(field)) {
                seen.add(field);
            }
        }, interval(bucket), source, Timestamp.from(from), Timestamp.from(to),
                allFields, allFields ? new String[0] : fields.toArray(String[]::new));

        List<Dtos.Matrix.Row> rows = values.entrySet().stream()
                .map(e -> new Dtos.Matrix.Row(e.getKey(), counts.getOrDefault(e.getKey(), 0L), e.getValue()))
                .toList();

        List<String> columns = allFields ? seen.stream().sorted().toList() : fields;
        return new Dtos.Matrix(source, bucket.id(), from, to, columns, rows);
    }

    public List<Dtos.RatePoint> rates(String source, Instant from, Instant to, Buckets bucket) {
        // Το records_1m είναι λεπτιαίο· τα χονδρότερα διαστήματα αθροίζονται από αυτό.
        return jdbc.query("""
                SELECT time_bucket(?::interval, bucket) AS t, sum(n) AS n
                FROM records_1m
                WHERE (?::text IS NULL OR source_id = ?::text) AND bucket >= ? AND bucket < ?
                GROUP BY t ORDER BY t
                """, (rs, i) -> new Dtos.RatePoint(instant(rs.getTimestamp("t")), rs.getLong("n")),
                interval(bucket), source, source,
                Timestamp.from(from), Timestamp.from(to));
    }

    // ---------------------------------------------------------------- ωμές εγγραφές

    public Dtos.RecordPage records(String source, String topic, Instant from, Instant to,
                                   Cursor cursor, int limit) {
        int size = Math.min(Math.max(limit, 1), MAX_PAGE);
        // Ένα παραπάνω, για να ξέρουμε αν υπάρχει επόμενη σελίδα χωρίς δεύτερο query.
        List<Dtos.RecordPage.Entry> found = jdbc.query("""
                SELECT event_time, source_id, topic, kafka_partition, kafka_offset, fields
                FROM ingested_records
                WHERE (?::text IS NULL OR source_id = ?::text)
                  AND (?::text IS NULL OR topic = ?::text)
                  AND event_time >= ? AND event_time < ?
                  AND (?::timestamptz IS NULL OR
                       (event_time, topic, kafka_partition, kafka_offset)
                       < (?::timestamptz, ?::text, ?::int, ?::bigint))
                ORDER BY event_time DESC, topic DESC, kafka_partition DESC, kafka_offset DESC
                LIMIT ?
                """, (rs, i) -> new Dtos.RecordPage.Entry(
                        instant(rs.getTimestamp("event_time")), rs.getString("source_id"),
                        rs.getString("topic"), rs.getInt("kafka_partition"), rs.getLong("kafka_offset"),
                        parseJson(rs.getString("fields"))),
                source, source, topic, topic, Timestamp.from(from), Timestamp.from(to),
                cursor == null ? null : Timestamp.from(cursor.eventTime()),
                cursor == null ? null : Timestamp.from(cursor.eventTime()),
                cursor == null ? null : cursor.topic(),
                cursor == null ? null : cursor.partition(),
                cursor == null ? null : cursor.offset(),
                size + 1);

        boolean hasMore = found.size() > size;
        List<Dtos.RecordPage.Entry> page = hasMore ? found.subList(0, size) : found;
        String next = null;
        if (hasMore) {
            Dtos.RecordPage.Entry last = page.get(page.size() - 1);
            next = new Cursor(last.eventTime(), last.topic(), last.partition(), last.offset()).encode();
        }
        return new Dtos.RecordPage(List.copyOf(page), next, hasMore);
    }

    // ---------------------------------------------------------------- βοηθητικά

    /** Το πλάτος του κουβά ως interval της Postgres. */
    private static String interval(Buckets bucket) {
        return bucket.width().getSeconds() + " seconds";
    }

    static Dtos.Point point(Instant t, long n, double sumV, double sumSq, double min, double max) {
        Double avg = n > 0 ? round(sumV / n) : null;
        Double stddev = null;
        if (n > 1) {
            // Από sum και sum-of-squares, ώστε να μπορεί να υπολογιστεί σε κάθε επίπεδο
            // συγκέντρωσης (ο μέσος όρος τυπικών αποκλίσεων δεν βγάζει νόημα).
            double variance = (sumSq - (sumV * sumV) / n) / (n - 1);
            stddev = round(Math.sqrt(Math.max(variance, 0)));
        }
        return new Dtos.Point(t, n, avg, round(min), round(max), stddev);
    }

    private static Double round(double v) {
        return Double.isFinite(v) ? Math.round(v * 1_000_000.0) / 1_000_000.0 : null;
    }

    private static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    private static Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return JSON.readValue(json, MAP);
        } catch (RuntimeException e) {
            // Jackson 3: οι εξαιρέσεις είναι unchecked
            log.warn("Μη αναγνώσιμο jsonb: {}", e.getMessage());
            return Map.of();
        }
    }
}
