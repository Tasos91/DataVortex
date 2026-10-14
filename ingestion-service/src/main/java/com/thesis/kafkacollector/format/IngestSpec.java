package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Το πλήρες σχέδιο μιας εισαγωγής, για <b>οποιαδήποτε</b> μορφή δεδομένων.
 * <p>
 * Ένα μοντέλο για όλα: CSV, JSON, NDJSON, XML. Ό,τι διαφέρει ανά μορφή ζει στο {@link Options},
 * ό,τι είναι κοινό —πού είναι οι εγγραφές, ποια πεδία κρατάμε, πού πάνε— ζει εδώ.
 *
 * <pre>
 * {
 *   "sourceId": "weather", "topic": "ingest.weather", "createTopicIfMissing": true,
 *   "format": "json",
 *   "recordPath": "$.data.stations[*].readings[*]",   ← πού είναι οι εγγραφές
 *   "timeField": "$.ts",
 *   "keyField":  "$$.data.stations[0].id",
 *   "fields": [
 *     {"name": "TempC",   "path": "$.temp_f", "type": "number", "scale": 0.5556, "offset": -17.78},
 *     {"name": "Humidity","path": "$.sensors.hum", "type": "number"},
 *     {"name": "Station", "path": "$$.station.id", "type": "text"}
 *   ],
 *   "storeRawPayload": false
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestSpec(
        String sourceId,
        String topic,
        boolean createTopicIfMissing,
        Integer partitions,
        /** auto | csv | json | ndjson | xml. Κενό σημαίνει «βρες το μόνος σου». */
        String format,
        /** Πού μέσα στο έγγραφο είναι οι εγγραφές· δες {@link RecordPath}. Για XML, το όνομα του στοιχείου. */
        String recordPath,
        List<FieldSpec> fields,
        /** Διαδρομή (ή όνομα στήλης) για τον χρόνο του γεγονότος. */
        String timeField,
        /** Ρητή μορφή χρόνου, όταν η αυτόματη ανάγνωση δεν αρκεί ({@code dd/MM/yyyy}). */
        String timeFormat,
        /** Διαδρομή (ή όνομα στήλης) για το κλειδί του μηνύματος. */
        String keyField,
        /** Να κρατηθεί το ωμό έγγραφο μαζί; Default {@code false}: διπλασιάζει bytes και χώρο. */
        Boolean storeRawPayload,
        Options options) {

    /** Ό,τι αφορά μόνο μία μορφή. Για τις υπόλοιπες αγνοείται. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Options(
            /** CSV: "," ";" "\\t" "|" */
            String delimiter,
            /** CSV: "." ή "," — το ελληνικό Excel γράφει 23,5 */
            String decimal,
            /** CSV: υπάρχει γραμμή επικεφαλίδων; */
            Boolean hasHeader,
            /** CSV: κωδικοποίηση αρχείου, αν δεν είναι UTF-8 */
            String charset) {

        public static Options defaults() {
            return new Options(null, null, null, null);
        }

        public char delimiterChar() {
            if (delimiter == null || delimiter.isEmpty()) {
                return ',';
            }
            return switch (delimiter) {
                case "\\t", "tab", "TAB" -> '\t';
                default -> delimiter.charAt(0);
            };
        }

        public char decimalChar() {
            return decimal != null && !decimal.isEmpty() && decimal.charAt(0) == ',' ? ',' : '.';
        }

        public boolean header() {
            return hasHeader == null || hasHeader;
        }

        public java.nio.charset.Charset charsetOrUtf8() {
            if (charset == null || charset.isBlank()) {
                return java.nio.charset.StandardCharsets.UTF_8;
            }
            try {
                return java.nio.charset.Charset.forName(charset.strip());
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Άγνωστη κωδικοποίηση '" + charset + "'.");
            }
        }
    }

    public Options opts() {
        return options == null ? Options.defaults() : options;
    }

    public Format formatOrDetect(String fileName, String sample) {
        Format declared = Format.parse(format);
        return declared != null ? declared : Format.detect(fileName, sample);
    }

    public boolean keepRaw() {
        return Boolean.TRUE.equals(storeRawPayload);
    }

    public String sourceIdOr(String fallback) {
        return sourceId != null && !sourceId.isBlank() ? sourceId.strip() : fallback;
    }

    /**
     * Ο έλεγχος που αξίζει να γίνει πριν διαβαστεί έστω μία εγγραφή: ένα λάθος εδώ θα έτρεχε
     * κανονικά και θα έγραφε εκατομμύρια άδειες εγγραφές.
     *
     * @param available τα διαθέσιμα ονόματα/διαδρομές, όταν τα ξέρουμε (CSV)· αλλιώς {@code null}
     */
    public void validate(List<String> available) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("Δεν διάλεξες κανένα πεδίο. Στο fields βάζεις όσα "
                    + "θέλεις να κρατηθούν, π.χ. [{\"name\":\"Temp\",\"path\":\"$.temp\",\"type\":\"number\"}].");
        }
        List<String> names = new ArrayList<>(fields.size());
        for (FieldSpec field : fields) {
            if (names.contains(field.name())) {
                throw new IllegalArgumentException("Το πεδίο '" + field.name() + "' δηλώθηκε δύο φορές.");
            }
            names.add(field.name());
            field.compile(); // πετά καθαρό σφάλμα αν η διαδρομή ή η μορφή χρόνου είναι άκυρη
            if (available != null && !field.fromRoot() && !available.contains(stripDollar(field.path()))) {
                throw new IllegalArgumentException("Η στήλη '" + field.path() + "' δεν υπάρχει στο αρχείο. "
                        + "Υπάρχουν: " + available);
            }
        }
        if (recordPath != null && !recordPath.isBlank()) {
            RecordPath.of(recordPath); // πετά αν η σύνταξη είναι άκυρη
        }
        if (available != null) {
            requireColumn(timeField, "χρόνου", available);
            requireColumn(keyField, "κλειδιού", available);
        }
    }

    private static void requireColumn(String field, String what, List<String> available) {
        if (field != null && !field.isBlank() && !available.contains(stripDollar(field))) {
            throw new IllegalArgumentException("Η στήλη " + what + " '" + field + "' δεν υπάρχει στο αρχείο.");
        }
    }

    /** Για CSV, το {@code $.col} και το {@code col} είναι το ίδιο πράγμα: όνομα στήλης. */
    public static String stripDollar(String path) {
        if (path == null) {
            return null;
        }
        String p = path.strip();
        return p.startsWith("$.") ? p.substring(2) : p;
    }

    /** Οι μορφές που αναγνωρίζει ο σωλήνας. */
    public enum Format {
        CSV, JSON, NDJSON, XML;

        public static Format parse(String name) {
            if (name == null || name.isBlank() || "auto".equalsIgnoreCase(name.strip())) {
                return null;
            }
            return switch (name.strip().toLowerCase(Locale.ROOT)) {
                case "csv", "tsv", "text/csv" -> CSV;
                case "json", "application/json" -> JSON;
                case "ndjson", "jsonl", "jsonlines", "application/x-ndjson" -> NDJSON;
                case "xml", "application/xml", "text/xml" -> XML;
                default -> throw new IllegalArgumentException("Άγνωστη μορφή '" + name
                        + "'. Δεκτές: csv, json, ndjson, xml, auto.");
            };
        }

        /**
         * Μαντεύει τη μορφή από το όνομα του αρχείου και το περιεχόμενο. Το περιεχόμενο κερδίζει:
         * ένα αρχείο με κατάληξη {@code .txt} που περιέχει JSON είναι JSON.
         */
        public static Format detect(String fileName, String sample) {
            String head = sample == null ? "" : sample.stripLeading();
            if (!head.isEmpty()) {
                char first = head.charAt(0);
                if (first == '<') {
                    return XML;
                }
                if (first == '{' || first == '[') {
                    // Πολλά αντικείμενα, ένα ανά γραμμή, σημαίνει NDJSON — όχι ένα μεγάλο JSON.
                    return looksLikeNdjson(head) ? NDJSON : JSON;
                }
                return CSV;
            }
            String name = fileName == null ? "" : fileName.toLowerCase(Locale.ROOT);
            if (name.endsWith(".xml")) return XML;
            if (name.endsWith(".ndjson") || name.endsWith(".jsonl")) return NDJSON;
            if (name.endsWith(".json")) return JSON;
            return CSV;
        }

        private static boolean looksLikeNdjson(String head) {
            int newline = head.indexOf('\n');
            if (newline < 0 || newline + 1 >= head.length()) {
                return false;
            }
            // Η γραμμή κλείνει και η επόμενη ανοίγει δικό της αντικείμενο: ένα ανά γραμμή.
            String firstLine = head.substring(0, newline).strip();
            String secondLine = head.substring(newline + 1).stripLeading();
            return firstLine.endsWith("}") && !secondLine.isEmpty() && secondLine.charAt(0) == '{';
        }
    }
}
