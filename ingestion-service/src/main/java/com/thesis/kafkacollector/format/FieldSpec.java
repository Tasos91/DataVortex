package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Value;
import com.thesis.kafkacollector.csv.CsvValues;
import com.thesis.kafkacollector.mapping.CompiledPath;
import com.thesis.kafkacollector.mapping.RecordMapper;
import com.thesis.kafkacollector.mapping.Values;

import java.time.format.DateTimeFormatter;

/**
 * Ένα πεδίο, όπως το ορίζει ο χρήστης: από πού έρχεται, πώς λέγεται, τι τύπο έχει, και τι
 * μετατροπή χρειάζεται για να γίνει χρήσιμο.
 *
 * <pre>
 * {"name": "TempC", "path": "$.sensor.temp_f", "type": "number",
 *  "scale": 0.5556, "offset": -17.78}          ← Fahrenheit σε Celsius
 *
 * {"name": "Station", "path": "$$.station.id"} ← από τη ΡΙΖΑ του εγγράφου, όχι από την εγγραφή
 *
 * {"name": "Ts", "path": "$.when", "type": "timestamp", "format": "dd/MM/yyyy HH:mm"}
 * </pre>
 *
 * <b>Γιατί μετασχηματισμοί εδώ και όχι μετά:</b> μια θερμοκρασία σε Fahrenheit, μια πίεση σε
 * Pascal αντί για hPa ή ένα ποσοστό στο 0–1 αντί για 0–100 δεν είναι «λάθος δεδομένα» — είναι
 * άλλη μονάδα. Αν η μετατροπή δεν γίνει στην είσοδο, κουβαλιέται για πάντα: στη βάση, στα
 * συγκεντρωτικά, στα κατώφλια της ανάλυσης, στα γραφήματα.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FieldSpec(
        String name,
        /** Διαδρομή από την εγγραφή. Με πρόθεμα {@code $$} μετράει από τη ρίζα του εγγράφου. */
        String path,
        /** number | text | bool | timestamp | auto (default: auto — ό,τι λέει η ίδια η τιμή) */
        String type,
        /** Πολλαπλασιαστής για αριθμούς, πριν το offset. */
        Double scale,
        /** Σταθερά που προστίθεται μετά το scale. */
        Double offset,
        /** Ρητή μορφή χρόνου, για τις αμφίσημες περιπτώσεις ({@code dd/MM} vs {@code MM/dd}). */
        String format,
        /** Τιμή όταν το πεδίο λείπει. Χωρίς αυτήν, το πεδίο απλώς δεν υπάρχει στην εγγραφή. */
        String defaultValue) {

    /** Το πρόθεμα που σημαίνει «ψάξε από τη ρίζα του εγγράφου, όχι από την εγγραφή». */
    public static final String ROOT_PREFIX = "$$";
    /**
     * Το πρόθεμα που σημαίνει «ψάξε στο αντικείμενο που ΠΕΡΙΕΧΕΙ αυτή την εγγραφή».
     * <p>
     * Σε {@code $.stations[*].readings[*]}, το {@code $^.id} είναι το id του σταθμού στον οποίο
     * ανήκει η μέτρηση. Περισσότερα {@code ^} ανεβαίνουν περισσότερα επίπεδα ({@code $^^.region}).
     * Η ρίζα ({@code $$}) δεν θα αρκούσε: με τρεις σταθμούς δεν ξέρει ποιος είναι ο σωστός.
     */
    public static final char PARENT_MARK = '^';

    public enum Kind { NUMBER, TEXT, BOOL, TIMESTAMP, AUTO }

    public FieldSpec {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Κάθε πεδίο χρειάζεται name.");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("Το πεδίο '" + name + "' δεν έχει path.");
        }
    }

    /** Σύντομη μορφή για τα απλά: όνομα και διαδρομή. */
    public static FieldSpec of(String name, String path, String type) {
        return new FieldSpec(name, path, type, null, null, null, null);
    }

    public Kind kind() {
        if (type == null || type.isBlank()) {
            return Kind.AUTO;
        }
        return switch (type.strip().toLowerCase(java.util.Locale.ROOT)) {
            case "number", "numeric", "double", "int", "integer", "metric", "float" -> Kind.NUMBER;
            case "text", "string", "label", "category" -> Kind.TEXT;
            case "bool", "boolean", "flag" -> Kind.BOOL;
            case "time", "timestamp", "date", "datetime" -> Kind.TIMESTAMP;
            default -> Kind.AUTO;
        };
    }

    /** Διαβάζει από τη ρίζα του εγγράφου αντί από την εγγραφή; */
    public boolean fromRoot() {
        return path.startsWith(ROOT_PREFIX);
    }

    /** Πόσα επίπεδα πάνω από την εγγραφή· 0 σημαίνει «μέσα στην ίδια». */
    public int parentLevels() {
        if (fromRoot() || path.length() < 2 || path.charAt(0) != '$') {
            return 0;
        }
        int levels = 0;
        while (1 + levels < path.length() && path.charAt(1 + levels) == PARENT_MARK) {
            levels++;
        }
        return levels;
    }

    /** Η διαδρομή χωρίς τα προθέματα, έτοιμη για μεταγλώττιση. */
    public String effectivePath() {
        if (fromRoot()) {
            return path.substring(ROOT_PREFIX.length() - 1);
        }
        int levels = parentLevels();
        return levels == 0 ? path : "$" + path.substring(1 + levels);
    }

    /** Ό,τι είναι σταθερό για όλη την εισαγωγή, υπολογισμένο μία φορά. */
    public Compiled compile() {
        CompiledPath compiled = CompiledPath.of(effectivePath());
        if (compiled == null) {
            throw new IllegalArgumentException("Άκυρη διαδρομή στο πεδίο '" + name + "': " + path);
        }
        DateTimeFormatter formatter = null;
        if (format != null && !format.isBlank()) {
            try {
                formatter = DateTimeFormatter.ofPattern(format.strip());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Άκυρο format στο πεδίο '" + name + "': " + e.getMessage());
            }
        }
        return new Compiled(this, compiled, formatter);
    }

    /**
     * Το πεδίο έτοιμο για χρήση: διαδρομή μεταγλωττισμένη, μορφή χρόνου αναλυμένη, τύπος
     * αποφασισμένος. Μέσα στον βρόχο των εγγραφών δεν γίνεται τίποτα από αυτά ξανά.
     */
    public static final class Compiled {

        private final FieldSpec spec;
        private final CompiledPath path;
        private final DateTimeFormatter formatter;
        private final Kind kind;
        private final boolean fromRoot;
        private final int parentLevels;
        private final double scale;
        private final double offset;
        private final boolean transforms;

        private Compiled(FieldSpec spec, CompiledPath path, DateTimeFormatter formatter) {
            this.spec = spec;
            this.path = path;
            this.formatter = formatter;
            this.kind = spec.kind();
            this.fromRoot = spec.fromRoot();
            this.parentLevels = spec.parentLevels();
            this.scale = spec.scale() == null ? 1.0 : spec.scale();
            this.offset = spec.offset() == null ? 0.0 : spec.offset();
            this.transforms = scale != 1.0 || offset != 0.0;
        }

        public String name() {
            return spec.name();
        }

        public boolean fromRoot() {
            return fromRoot;
        }

        public int parentLevels() {
            return parentLevels;
        }

        /** Τα βήματα της διαδρομής, για το δέντρο κοινών προθεμάτων. */
        public String[] steps() {
            return path.steps();
        }

        /**
         * Μετατρέπει έναν κόμβο που <b>έχει ήδη βρεθεί</b> — η διάσχιση έγινε από το δέντρο.
         *
         * @return η τιμή, η προεπιλογή αν ο κόμβος λείπει, ή {@code null} αν δεν υπάρχει ούτε αυτή
         */
        public Value convertOrDefault(JsonNode node) {
            if (node == null || node.isNull() || node.isMissingNode()) {
                return spec.defaultValue() == null ? null : convertText(spec.defaultValue());
            }
            return convert(node);
        }

        /**
         * Σε ποιο αντικείμενο ψάχνει αυτό το πεδίο: στην εγγραφή, σε πρόγονό της, ή στη ρίζα.
         * <p>
         * Όταν ο ζητούμενος πρόγονος δεν υπάρχει, το αποτέλεσμα είναι {@code null} και το πεδίο
         * <b>λείπει</b>. ΜΗΝ βάλεις εδώ υποχώρηση στην ίδια την εγγραφή: ένα {@code $^.host} θα
         * διάβαζε σιωπηλά το {@code $.host} και θα έδινε λάθος δεδομένα που μοιάζουν σωστά.
         */
        private JsonNode base(JsonNode record, RecordPath.Scope scope) {
            if (fromRoot) {
                return scope == null ? null : scope.root();
            }
            if (parentLevels == 0) {
                return record;
            }
            return scope == null ? null : scope.ancestor(parentLevels);
        }

        public Value read(JsonNode record, RecordPath.Scope scope) {
            JsonNode base = base(record, scope);
            JsonNode node = base == null ? null : path.resolve(base);
            if (node == null || node.isNull()) {
                return spec.defaultValue() == null ? null : convertText(spec.defaultValue());
            }
            return convert(node);
        }

        public Long readTime(JsonNode record, RecordPath.Scope scope) {
            JsonNode base = base(record, scope);
            JsonNode node = base == null ? null : path.resolve(base);
            if (node == null || node.isNull()) {
                return null;
            }
            return formatter != null ? CsvValues.parseTime(node.asText(), formatter)
                    : RecordMapper.toEpochMillis(node);
        }

        public String readKey(JsonNode record, RecordPath.Scope scope) {
            JsonNode base = base(record, scope);
            JsonNode node = base == null ? null : path.resolve(base);
            if (node == null || node.isNull()) {
                return null;
            }
            String key = node.isValueNode() ? node.asText() : node.toString();
            return key.isEmpty() ? null : key;
        }

        /**
         * @param record η τρέχουσα εγγραφή
         * @param root   η ρίζα του εγγράφου, για τα πεδία με {@code $$}
         * @return η τιμή, ή {@code null} αν λείπει ή δεν ταιριάζει στον τύπο
         */

        private Value convert(JsonNode node) {
            return switch (kind) {
                case NUMBER -> {
                    double d = node.isNumber() ? node.asDouble()
                            : CsvValues.parseNumber(node.asText());
                    yield Double.isNaN(d) ? null : Values.ofNumber(apply(d));
                }
                case TEXT -> Values.ofString(node.isValueNode() ? node.asText() : node.toString());
                case BOOL -> {
                    if (node.isBoolean()) {
                        yield Values.ofBoolean(node.asBoolean());
                    }
                    Boolean b = CsvValues.parseBool(node.asText());
                    yield b == null ? null : Values.ofBoolean(b);
                }
                case TIMESTAMP -> {
                    Long millis = formatter != null
                            ? CsvValues.parseTime(node.asText(), formatter)
                            : RecordMapper.toEpochMillis(node);
                    yield millis == null ? null : Values.ofNumber(millis);
                }
                // AUTO: ό,τι λέει η ίδια η τιμή. Οι μετασχηματισμοί ισχύουν μόνο αν είναι αριθμός.
                case AUTO -> transforms && node.isNumber()
                        ? Values.ofNumber(apply(node.asDouble()))
                        : Values.of(node);
            };
        }

        /** Η προεπιλογή δίνεται ως κείμενο· περνά από την ίδια μετατροπή με ό,τι βρίσκεται. */
        private Value convertText(String text) {
            return convert(com.fasterxml.jackson.databind.node.TextNode.valueOf(text));
        }

        private double apply(double d) {
            return transforms ? d * scale + offset : d;
        }

        /** Ο χρόνος του γεγονότος από αυτό το πεδίο, σε epoch millis. */

        /** Το κλειδί του μηνύματος από αυτό το πεδίο. */

    }
}
