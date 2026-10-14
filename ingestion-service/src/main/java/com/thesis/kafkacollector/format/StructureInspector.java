package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Κοιτάζει ένα δείγμα από <b>οποιαδήποτε</b> δομή και απαντά στις δύο ερωτήσεις που έχει ο χρήστης
 * μπροστά του: <b>«πού είναι οι εγγραφές;»</b> και <b>«τι πεδία έχουν μέσα;»</b>
 * <p>
 * Είναι το αντίστοιχο του {@code CsvInspector} για τις δομημένες μορφές. Το δύσκολο κομμάτι εδώ
 * δεν είναι τα πεδία αλλά το <b>πού ξεκινούν οι εγγραφές</b>: τα αληθινά δεδομένα έρχονται
 * τυλιγμένα σε επίπεδα απάντησης ({@code status}, {@code page}, {@code data}, {@code results}),
 * και το ζητούμενο βρίσκεται κάπου μέσα.
 * <p>
 * Η ευρετική είναι απλή και δουλεύει: <b>ο μεγαλύτερος πίνακας αντικειμένων είναι οι εγγραφές</b>.
 * Ό,τι άλλο υπάρχει γύρω είναι μεταδεδομένα της απάντησης.
 */
public final class StructureInspector {

    /** Πόσες εγγραφές φτάνουν για να κριθεί ο τύπος ενός πεδίου. */
    public static final int MAX_RECORDS = 200;
    private static final int MAX_DISTINCT = 1000;
    private static final int MAX_DEPTH = 12;

    private StructureInspector() {
    }

    /** Το προφίλ ενός πεδίου: τι είναι και πόσο αξιόπιστο είναι αυτό που βλέπουμε. */
    public record Field(String path,
                        String type,
                        int present,
                        int missing,
                        int distinct,
                        Double min,
                        Double max,
                        List<String> samples,
                        /** metric | label | time | key | skip */
                        String role) {
    }

    public record Profile(String format,
                          String recordPath,
                          int recordsSampled,
                          List<Field> fields,
                          IngestSpec suggestion,
                          List<String> notes) {
    }

    /**
     * @param sample   το δείγμα του εγγράφου (οι πρώτες γραμμές/bytes αρκούν)
     * @param format   η μορφή, ήδη αποφασισμένη
     * @param fileName για να προταθεί sourceId/topic· μπορεί να είναι {@code null}
     */
    public static Profile inspect(String sample, IngestSpec.Format format, String fileName,
                                  ObjectMapper mapper) throws Exception {
        List<String> notes = new ArrayList<>();
        List<JsonNode> records = new ArrayList<>();
        /** Τα αντικείμενα που περιέχουν τις εγγραφές: εκεί ζει συνήθως η ταυτότητα της οντότητας. */
        List<JsonNode> parents = new ArrayList<>();
        String recordPath;

        switch (format) {
            case NDJSON -> {
                recordPath = "$";
                for (String line : sample.split("\n")) {
                    if (records.size() >= MAX_RECORDS) {
                        break;
                    }
                    String trimmed = line.strip();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    try {
                        records.add(mapper.readTree(trimmed));
                    } catch (Exception e) {
                        // Η τελευταία γραμμή ενός κομμένου δείγματος είναι συχνά μισή: δεν πειράζει.
                    }
                }
            }
            case XML -> {
                String element = biggestRepeatedElement(sample);
                recordPath = element == null ? "" : element;
                try {
                    XmlRecords.forEachWithScope(new StringReader(sample), element, (node, scope) -> {
                        if (records.size() < MAX_RECORDS) {
                            records.add(node);
                            JsonNode parent = scope.ancestor(1);
                            if (parent != null && !parent.isEmpty()) {
                                parents.add(parent);
                            }
                        }
                    });
                } catch (Exception e) {
                    // Κομμένο δείγμα: κρατάμε όσα πρόλαβαν να διαβαστούν
                    if (records.isEmpty()) {
                        throw new IllegalArgumentException("Το XML δεν διαβάστηκε: " + e.getMessage());
                    }
                }
            }
            default -> { // JSON
                JsonNode root = mapper.readTree(sample);
                if (root == null || root.isMissingNode()) {
                    throw new IllegalArgumentException("Το έγγραφο είναι κενό ή δεν είναι έγκυρο JSON.");
                }
                recordPath = findRecordPath(root);
                RecordPath.of(recordPath).forEachWithScope(root, (node, scope) -> {
                    if (records.size() < MAX_RECORDS) {
                        records.add(node);
                        JsonNode parent = scope.ancestor(1);
                        if (parent != null) {
                            parents.add(parent);
                        }
                    }
                });
                if (!"$".equals(recordPath)) {
                    notes.add("Οι εγγραφές βρέθηκαν στο " + recordPath
                            + ". Αν δεν είναι αυτές που θες, άλλαξε το recordPath.");
                }
            }
        }

        if (records.isEmpty()) {
            throw new IllegalArgumentException("Δεν βρέθηκε καμία εγγραφή στο δείγμα.");
        }

        List<Field> fields = profileFields(records);
        // Τα πεδία του γονέα μπαίνουν με πρόθεμα «$^»: σε «σταθμός με λίστα μετρήσεων», το όνομα
        // του σταθμού είναι ΕΞΩ από τη μέτρηση, και χωρίς αυτό χάνεται η ταυτότητα της οντότητας.
        if (!parents.isEmpty()) {
            for (Field parentField : profileParents(parents)) {
                fields.add(parentField);
            }
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Οι εγγραφές δεν έχουν πεδία που να μπορούν να διαβαστούν.");
        }
        long metrics = fields.stream().filter(f -> "metric".equals(f.role())).count();
        if (metrics == 0) {
            notes.add("Κανένα αριθμητικό πεδίο: θα αποθηκευτούν δεδομένα αλλά δεν θα υπάρχουν μετρικές.");
        }
        if (fields.stream().noneMatch(f -> "time".equals(f.role()))) {
            notes.add("Κανένα πεδίο δεν φαίνεται να είναι χρόνος. Χωρίς timeField όλες οι εγγραφές "
                    + "παίρνουν την ώρα της εισαγωγής, άρα δεν υπάρχει χρονοσειρά.");
        }
        return new Profile(format.name().toLowerCase(Locale.ROOT), recordPath, records.size(), fields,
                suggest(fileName, format, recordPath, fields), notes);
    }

    // ------------------------------------------------------------ πού είναι οι εγγραφές

    /**
     * Ο μεγαλύτερος πίνακας αντικειμένων μέσα στο έγγραφο. Αν δεν υπάρχει κανένας, η ρίζα.
     */
    static String findRecordPath(JsonNode root) {
        Best best = new Best();
        search(root, "$", 0, 1, best);
        return best.path == null ? "$" : best.path + "[*]";
    }

    private static final class Best {
        String path;
        long size = 1; // ένας πίνακας με ένα στοιχείο δεν είναι αρκετός λόγος
    }

    /**
     * @param multiplier πόσες φορές εμφανίζεται αυτό το σημείο, λόγω των ξεδιπλωμάτων από πάνω του.
     *                   Σε {@code stations[*].readings}, ο κάθε {@code readings} μετράει επί το
     *                   πλήθος των σταθμών — αλλιώς ένας πίνακας 3 σταθμών θα κέρδιζε από τις
     *                   240 μετρήσεις που έχει ο καθένας, και θα κρατούσαμε λάθος επίπεδο.
     */
    private static void search(JsonNode node, String path, int depth, long multiplier, Best best) {
        if (depth > MAX_DEPTH || node == null) {
            return;
        }
        if (node.isArray()) {
            int objects = 0;
            for (JsonNode child : node) {
                if (child.isObject()) {
                    objects++;
                }
            }
            long total = (long) objects * multiplier;
            if (total > best.size) {
                best.size = total;
                best.path = path;
            }
            // Μέσα στα στοιχεία μπορεί να κρύβεται μεγαλύτερος πίνακας. Κατεβαίνουμε με «[*]»
            // και όχι «[0]»: οι εγγραφές είναι ΟΛΩΝ των στοιχείων, όχι μόνο του πρώτου.
            if (!node.isEmpty()) {
                search(node.get(0), path + "[*]", depth + 1, Math.max(1, total), best);
            }
        } else if (node.isObject()) {
            node.properties().forEach(entry ->
                    search(entry.getValue(), path + "." + entry.getKey(), depth + 1, multiplier, best));
        }
    }

    /** Για XML: το όνομα στοιχείου που επαναλαμβάνεται περισσότερο — εκεί είναι οι εγγραφές. */
    static String biggestRepeatedElement(String xml) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("<([A-Za-z_][\\w.-]*)(\\s[^>]*)?>").matcher(xml);
        while (m.find()) {
            counts.merge(m.group(1), 1, Integer::sum);
        }
        return counts.entrySet().stream()
                .filter(e -> e.getValue() > 1)
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    // ------------------------------------------------------------ τι πεδία έχουν

    /** Ξεδιπλώνει κάθε εγγραφή σε διαδρομές φύλλων και μαζεύει στατιστικά ανά διαδρομή. */
    private static List<Field> profileFields(List<JsonNode> records) {
        Map<String, Stats> byPath = new LinkedHashMap<>();
        for (JsonNode record : records) {
            flatten(record, "$", 0, byPath);
        }
        List<Field> fields = new ArrayList<>(byPath.size());
        boolean timeTaken = false;
        boolean keyTaken = false;
        for (Map.Entry<String, Stats> entry : byPath.entrySet()) {
            Stats stats = entry.getValue();
            Field field = stats.toField(entry.getKey(), records.size(), timeTaken, keyTaken);
            if ("time".equals(field.role())) timeTaken = true;
            if ("key".equals(field.role())) keyTaken = true;
            fields.add(field);
        }
        return fields;
    }

    /**
     * Τα <b>βαθμωτά</b> πεδία των γονέων, ως διαδρομές με {@code $^}. Μόνο τα βαθμωτά: οι λίστες
     * του γονέα είναι οι ίδιες οι εγγραφές, και θα ήταν παράλογο να μπουν και ως πεδίο.
     */
    private static List<Field> profileParents(List<JsonNode> parents) {
        Map<String, Stats> byPath = new LinkedHashMap<>();
        for (JsonNode parent : parents) {
            if (!parent.isObject()) {
                continue;
            }
            parent.properties().forEach(e -> {
                if (e.getValue().isValueNode()) {
                    byPath.computeIfAbsent("$^." + e.getKey(), k -> new Stats()).add(e.getValue());
                }
            });
        }
        List<Field> fields = new ArrayList<>(byPath.size());
        boolean keyTaken = false;
        for (Map.Entry<String, Stats> entry : byPath.entrySet()) {
            Field field = entry.getValue().toField(entry.getKey(), parents.size(), true, keyTaken);
            if ("key".equals(field.role())) {
                keyTaken = true;
            }
            fields.add(field);
        }
        // Ένα πεδίο του γονέα με λίγες διακριτές τιμές ΕΙΝΑΙ η οντότητα: ο σταθμός, ο διακομιστής,
        // το σύμβολο. Είναι το φυσικό κλειδί partition, και το δηλώνουμε ακόμα κι όταν το δείγμα
        // έδειξε μία μόνο τιμή — τα πρώτα 200 readings ενός αρχείου ανήκουν συχνά στον ίδιο γονέα,
        // που δεν σημαίνει ότι υπάρχει ένας.
        if (!keyTaken) {
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if ("label".equals(field.role()) && "text".equals(field.type())
                        && field.distinct() > 0 && field.distinct() <= Math.max(8, parents.size() / 5)) {
                    fields.set(i, new Field(field.path(), field.type(), field.present(), field.missing(),
                            field.distinct(), field.min(), field.max(), field.samples(), "key"));
                    break;
                }
            }
        }
        return fields;
    }

    private static void flatten(JsonNode node, String path, int depth, Map<String, Stats> out) {
        if (depth > MAX_DEPTH || node == null) {
            return;
        }
        if (node.isObject()) {
            node.properties().forEach(e -> flatten(e.getValue(), path + "." + e.getKey(), depth + 1, out));
        } else if (node.isArray()) {
            // Ένας πίνακας μέσα σε εγγραφή: κρατάμε το πρώτο στοιχείο ως δείγμα της δομής, και
            // τον ίδιο τον πίνακα ως πεδίο (θα αποθηκευτεί ως λίστα).
            if (!node.isEmpty() && node.get(0).isValueNode()) {
                out.computeIfAbsent(path, k -> new Stats()).add(node);
            } else if (!node.isEmpty()) {
                flatten(node.get(0), path + "[0]", depth + 1, out);
            }
        } else {
            out.computeIfAbsent(path, k -> new Stats()).add(node);
        }
    }

    /** Τα στατιστικά μιας διαδρομής σε όλες τις εγγραφές του δείγματος. */
    private static final class Stats {
        int present;
        int numbers;
        int times;
        int booleans;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        final Set<String> distinct = new LinkedHashSet<>();

        void add(JsonNode node) {
            if (node.isNull() || node.isMissingNode()) {
                return;
            }
            String text = node.isValueNode() ? node.asText() : node.toString();
            if (text.isBlank() || com.thesis.kafkacollector.csv.CsvValues.isMissing(text)) {
                return;
            }
            present++;
            if (distinct.size() < MAX_DISTINCT) {
                distinct.add(text);
            }
            if (node.isBoolean() || com.thesis.kafkacollector.csv.CsvValues.parseBool(text) != null) {
                booleans++;
            }
            double d = node.isNumber() ? node.asDouble() : com.thesis.kafkacollector.csv.CsvValues.parseNumber(text);
            if (!Double.isNaN(d)) {
                numbers++;
                min = Math.min(min, d);
                max = Math.max(max, d);
            }
            if (com.thesis.kafkacollector.csv.CsvValues.parseTime(text) != null) {
                times++;
            }
        }

        Field toField(String path, int records, boolean timeTaken, boolean keyTaken) {
            String type = decideType(path);
            String role = decideRole(type, records, timeTaken, keyTaken);
            List<String> samples = new ArrayList<>(distinct).subList(0, Math.min(4, distinct.size()));
            return new Field(path, type, present, records - present, distinct.size(),
                    numbers > 0 ? min : null, numbers > 0 ? max : null, List.copyOf(samples), role);
        }

        private String decideType(String path) {
            if (present == 0) {
                return "text";
            }
            if (booleans * 10 >= present * 9) {
                return "bool";
            }
            boolean allNumeric = numbers == present;
            if (times * 10 >= present * 9 && (!allNumeric || looksTemporal(path))) {
                return "timestamp";
            }
            if (numbers * 10 >= present * 9) {
                return "number";
            }
            return times * 10 >= present * 9 ? "timestamp" : "text";
        }

        /** Ένα epoch είναι και αριθμός και χρόνος· το κρίνει το όνομα μαζί με το εύρος. */
        private boolean looksTemporal(String path) {
            String p = path.toLowerCase(Locale.ROOT);
            boolean named = p.contains("time") || p.contains("date") || p.endsWith(".ts")
                    || p.contains("stamp") || p.contains("ώρα") || p.contains("ημερ") || p.contains("χρον");
            boolean inRange = (min >= 1.0e9 && max <= 4.0e9) || (min >= 1.0e12 && max <= 4.0e12);
            return named && inRange;
        }

        private String decideRole(String type, int records, boolean timeTaken, boolean keyTaken) {
            if (present == 0) {
                return "skip";
            }
            return switch (type) {
                case "timestamp" -> timeTaken ? "label" : "time";
                case "number" -> "metric";
                case "bool" -> "label";
                default -> !keyTaken && distinct.size() >= 2 && distinct.size() <= Math.max(8, records / 5)
                        ? "key" : "label";
            };
        }
    }

    // ------------------------------------------------------------ η πρόταση

    private static IngestSpec suggest(String fileName, IngestSpec.Format format, String recordPath,
                                      List<Field> fields) {
        String source = com.thesis.kafkacollector.csv.CsvInspector.sourceIdFrom(fileName);
        List<FieldSpec> chosen = new ArrayList<>();
        String time = null;
        String key = null;
        for (Field field : fields) {
            switch (field.role()) {
                case "time" -> time = field.path();
                case "metric" -> chosen.add(FieldSpec.of(name(field.path()), field.path(), "number"));
                case "key" -> {
                    key = field.path();
                    chosen.add(FieldSpec.of(name(field.path()), field.path(), "text"));
                }
                case "label" -> chosen.add(FieldSpec.of(name(field.path()), field.path(), field.type()));
                default -> { /* skip */ }
            }
        }
        return new IngestSpec(source, "ingest." + source, true, 6,
                format.name().toLowerCase(Locale.ROOT), recordPath, chosen,
                time, null, key, false, IngestSpec.Options.defaults());
    }

    /** Το όνομα του πεδίου στο Protobuf: το τελευταίο κομμάτι της διαδρομής, χωρίς το «$.». */
    static String name(String path) {
        String p = path.startsWith("$.") ? path.substring(2) : path;
        int dot = p.lastIndexOf('.');
        String last = dot >= 0 ? p.substring(dot + 1) : p;
        return last.isEmpty() ? p : last;
    }
}
