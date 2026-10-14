package com.thesis.kafkacollector.csv;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Το σχέδιο εισαγωγής ενός CSV: <b>τι διάλεξε ο χρήστης</b> αφού είδε τι έχει μέσα το αρχείο.
 * <p>
 * Στο {@code columns} μπαίνουν <b>μόνο</b> οι στήλες που θέλει· οι υπόλοιπες δεν φτάνουν ποτέ
 * στο Kafka — ούτε ως bytes. Κάθε στήλη λέει με ποιο όνομα θα ζήσει στο Protobuf ({@code as})
 * και με ποιον τύπο ({@code type}), γιατί από τον τύπο κρίνεται αν θα γίνει μετρική.
 *
 * <pre>
 * {
 *   "sourceId": "weather-athens",
 *   "topic": "ingest.weather", "createTopicIfMissing": true, "partitions": 6,
 *   "delimiter": ",", "hasHeader": true,
 *   "timeColumn": "timestamp", "keyColumn": "station",
 *   "columns": [ {"name": "temp_c", "as": "TempC", "type": "number"},
 *                {"name": "station", "as": "Station", "type": "text"} ]
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CsvPlan(
        String sourceId,
        String topic,
        boolean createTopicIfMissing,
        Integer partitions,
        /** ",", ";", tab ή "|". Κενό = ό,τι βρήκε ο {@link CsvInspector}. */
        String delimiter,
        /**
         * Το δεκαδικό σύμβολο: "." ή ",". Ένα ελληνικό ή γερμανικό Excel γράφει {@code 23,5} και
         * χωρίς αυτό η στήλη διαβάζεται ως 235 — λάθος κατά μία τάξη μεγέθους, σιωπηλά.
         */
        String decimal,
        Boolean hasHeader,
        /** Η στήλη που δίνει τον χρόνο του γεγονότος. Χωρίς αυτήν όλα φαίνονται «τώρα». */
        String timeColumn,
        /** Ρητό pattern, για τις αμφίσημες μορφές όπως {@code dd/MM/yyyy}. */
        String timeFormat,
        /** Η στήλη που δίνει το κλειδί του μηνύματος (partition, άρα και σειρά ανά οντότητα). */
        String keyColumn,
        List<Column> columns,
        /**
         * Να κρατηθεί ολόκληρη η γραμμή CSV ως ωμό JSON. Default {@code false}: διπλασιάζει
         * bytes στο Kafka και στη βάση για κάτι που κανείς δεν διαβάζει.
         */
        Boolean rawPayload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Column(String name, String as, String type) {
        /** Το όνομα με το οποίο θα ζήσει το πεδίο στο Protobuf· default το όνομα της στήλης. */
        public String target() {
            return as != null && !as.isBlank() ? as.strip() : name;
        }

        public CsvValues.Type typeOrText() {
            if (type == null) return CsvValues.Type.TEXT;
            return switch (type.strip().toLowerCase()) {
                case "number", "numeric", "double", "int", "metric" -> CsvValues.Type.NUMBER;
                case "bool", "boolean" -> CsvValues.Type.BOOL;
                case "time", "timestamp", "date", "datetime" -> CsvValues.Type.TIMESTAMP;
                default -> CsvValues.Type.TEXT;
            };
        }
    }

    public char delimiterChar() {
        if (delimiter == null || delimiter.isEmpty()) return ',';
        String d = delimiter;
        return switch (d) {
            case "\\t", "tab", "TAB" -> '\t';
            default -> d.charAt(0);
        };
    }

    public char decimalChar() {
        return decimal != null && !decimal.isEmpty() && decimal.charAt(0) == ',' ? ',' : '.';
    }

    public boolean header() {
        return hasHeader == null || hasHeader;
    }

    public boolean keepRaw() {
        return Boolean.TRUE.equals(rawPayload);
    }

    public DateTimeFormatter timeFormatter() {
        return timeFormat == null || timeFormat.isBlank() ? null : DateTimeFormatter.ofPattern(timeFormat.strip());
    }

    /**
     * Ο έλεγχος που αξίζει να γίνει πριν διαβαστεί έστω μία γραμμή: μια εισαγωγή χωρίς στήλες ή με
     * λάθος όνομα στήλης θα έτρεχε κανονικά και θα έγραφε εκατομμύρια άδειες εγγραφές.
     *
     * @param available τα ονόματα των στηλών του αρχείου, για να ελεγχθεί ότι υπάρχουν όσα ζητήθηκαν
     */
    public void validate(List<String> available) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Δεν διάλεξες καμία στήλη. Στο columns βάζεις όσες "
                    + "θέλεις να γίνουν πεδία, π.χ. [{\"name\":\"temp_c\",\"type\":\"number\"}].");
        }
        for (Column c : columns) {
            if (c.name() == null || c.name().isBlank()) {
                throw new IllegalArgumentException("Στήλη χωρίς name στο columns.");
            }
            if (available != null && !available.contains(c.name())) {
                throw new IllegalArgumentException("Η στήλη '" + c.name() + "' δεν υπάρχει στο αρχείο. "
                        + "Υπάρχουν: " + available);
            }
        }
        if (available != null && timeColumn != null && !timeColumn.isBlank() && !available.contains(timeColumn)) {
            throw new IllegalArgumentException("Η στήλη χρόνου '" + timeColumn + "' δεν υπάρχει στο αρχείο.");
        }
        if (available != null && keyColumn != null && !keyColumn.isBlank() && !available.contains(keyColumn)) {
            throw new IllegalArgumentException("Η στήλη κλειδιού '" + keyColumn + "' δεν υπάρχει στο αρχείο.");
        }
        if (timeFormat != null && !timeFormat.isBlank()) {
            try {
                DateTimeFormatter.ofPattern(timeFormat.strip());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Άκυρο timeFormat '" + timeFormat + "': " + e.getMessage());
            }
        }
    }
}
