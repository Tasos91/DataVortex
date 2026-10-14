package com.thesis.kafkacollector.csv;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Κοιτάζει τις πρώτες γραμμές ενός CSV και απαντά στην ερώτηση που έχει ο χρήστης μπροστά του:
 * <b>«τι έχει μέσα αυτό το αρχείο και ποια πεδία αξίζει να κρατήσω;»</b>
 * <p>
 * Βρίσκει το διαχωριστικό, αν υπάρχει γραμμή επικεφαλίδων, τον τύπο κάθε στήλης, πόσα κενά και
 * πόσες διαφορετικές τιμές έχει, και επιστρέφει ένα <b>έτοιμο {@link CsvPlan}</b> που ο χρήστης
 * απλώς πειράζει. Χωρίς αυτό, το «γράψε μόνος σου τα fieldMappings» σημαίνει ότι πρέπει πρώτα να
 * ανοίξεις το αρχείο αλλού για να δεις πώς λέγονται οι στήλες.
 * <p>
 * Διαβάζει μόνο δείγμα ({@value #MAX_ROWS} γραμμές): η επιθεώρηση ενός αρχείου 2 GB κοστίζει όσο
 * και ενός 2 KB.
 */
public final class CsvInspector {

    /** Πόσες γραμμές φτάνουν για να κριθεί ο τύπος μιας στήλης. */
    public static final int MAX_ROWS = 200;
    private static final char[] CANDIDATES = {',', ';', '\t', '|'};
    private static final int MAX_DISTINCT = 1000;

    private CsvInspector() {}

    /** Το προφίλ μιας στήλης: τι είναι και πόσο αξιόπιστο είναι αυτό που βλέπουμε. */
    public record Column(int index,
                         String name,
                         String type,
                         /** Το δεκαδικό σύμβολο της στήλης: «.» ή «,». */
                         String decimal,
                         int present,
                         int missing,
                         int distinct,
                         Double min,
                         Double max,
                         List<String> samples,
                         /** Πρόταση: metric | label | time | key | skip */
                         String role) {
    }

    public record Profile(String delimiter,
                          boolean hasHeader,
                          int rowsSampled,
                          List<Column> columns,
                          CsvPlan suggestion,
                          List<String> notes) {
    }

    /**
     * @param text     το δείγμα του αρχείου (οι πρώτες γραμμές αρκούν)
     * @param fileName για να προταθεί sourceId/topic· μπορεί να είναι {@code null}
     */
    public static Profile inspect(String text, String fileName) throws IOException {
        String sample = CsvReader.stripBom(text == null ? "" : text);
        List<String> notes = new ArrayList<>();

        char delimiter = sniffDelimiter(sample);
        List<String[]> rows = read(sample, delimiter, MAX_ROWS + 1);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Το αρχείο δεν έχει καμία γραμμή.");
        }

        boolean hasHeader = sniffHeader(rows);
        String[] headerRow = rows.get(0);
        List<String[]> data = hasHeader ? rows.subList(1, rows.size()) : rows;

        int width = 0;
        for (String[] r : rows) {
            width = Math.max(width, r.length);
        }
        List<String> names = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            String n = hasHeader && i < headerRow.length ? headerRow[i].strip() : "";
            names.add(n.isEmpty() ? "col_" + (i + 1) : n);
        }
        dedupe(names);
        if (!hasHeader) {
            notes.add("Δεν βρέθηκε γραμμή επικεφαλίδων· οι στήλες ονομάστηκαν col_1..col_" + width + ".");
        }

        List<Column> columns = new ArrayList<>(width);
        boolean timeTaken = false;
        boolean keyTaken = false;
        for (int i = 0; i < width; i++) {
            Column c = profile(i, names.get(i), data, timeTaken, keyTaken);
            if ("time".equals(c.role())) timeTaken = true;
            if ("key".equals(c.role())) keyTaken = true;
            columns.add(c);
        }

        if (!timeTaken) {
            notes.add("Καμία στήλη δεν φαίνεται να είναι χρόνος. Χωρίς timeColumn όλες οι εγγραφές "
                    + "παίρνουν την ώρα της εισαγωγής, άρα δεν υπάρχει χρονοσειρά.");
        }
        long metrics = columns.stream().filter(c -> "metric".equals(c.role())).count();
        if (metrics == 0) {
            notes.add("Καμία αριθμητική στήλη: θα αποθηκευτούν δεδομένα αλλά δεν θα υπάρχουν μετρικές.");
        }
        return new Profile(label(delimiter), hasHeader, data.size(), columns,
                suggest(fileName, delimiter, hasHeader, columns), notes);
    }

    /**
     * Το σωστό διαχωριστικό δεν είναι «το πιο συχνό» αλλά αυτό που δίνει <b>σταθερό</b> πλήθος
     * στηλών σε κάθε γραμμή: ένα αρχείο με «;» γεμάτο κόμματα μέσα σε κείμενο θα διάλεγε το κόμμα
     * και θα έβγαζε άλλο πλήθος πεδίων ανά γραμμή.
     */
    static char sniffDelimiter(String sample) {
        char best = ',';
        double bestScore = -1;
        for (char d : CANDIDATES) {
            List<String[]> rows;
            try {
                rows = read(sample, d, 20);
            } catch (IOException e) {
                continue;
            }
            if (rows.isEmpty()) continue;
            int first = rows.get(0).length;
            if (first < 2) continue; // με ένα πεδίο δεν χώρισε τίποτα
            boolean stable = true;
            for (String[] r : rows) {
                if (r.length != first) {
                    stable = false;
                    break;
                }
            }
            double score = (stable ? 1000 : 0) + first;
            if (score > bestScore) {
                bestScore = score;
                best = d;
            }
        }
        return best;
    }

    /**
     * Επικεφαλίδες υπάρχουν όταν η πρώτη γραμμή είναι κείμενο αλλά κάτω από αυτή υπάρχει στήλη με
     * αριθμούς ή χρόνους: τότε η πρώτη γραμμή δεν είναι δεδομένο, είναι όνομα.
     */
    static boolean sniffHeader(List<String[]> rows) {
        if (rows.size() < 2) {
            return true; // μόνο μία γραμμή: πιο λογικό να είναι επικεφαλίδες παρά μοναδικό δεδομένο
        }
        String[] head = rows.get(0);
        for (int i = 0; i < head.length; i++) {
            boolean headerIsText = Double.isNaN(CsvValues.parseNumber(head[i])) && CsvValues.parseTime(head[i]) == null;
            if (!headerIsText) {
                return false; // η πρώτη γραμμή περιέχει τιμή, άρα είναι δεδομένο
            }
        }
        for (int i = 1; i < rows.size(); i++) {
            for (String v : rows.get(i)) {
                if (!CsvValues.isMissing(v)
                        && (!Double.isNaN(CsvValues.parseNumber(v)) || CsvValues.parseTime(v) != null)) {
                    return true;
                }
            }
        }
        // Όλα κείμενο: επικεφαλίδες αν η πρώτη γραμμή έχει μοναδικές, μη κενές τιμές
        Set<String> seen = new LinkedHashSet<>();
        for (String v : head) {
            if (v.isBlank() || !seen.add(v)) {
                return false;
            }
        }
        return true;
    }

    private static Column profile(int index, String name, List<String[]> data, boolean timeTaken, boolean keyTaken) {
        List<String> values = new ArrayList<>(data.size());
        int missing = 0;
        for (String[] r : data) {
            String v = index < r.length ? r[index] : null;
            if (CsvValues.isMissing(v)) {
                missing++;
            } else {
                values.add(v);
            }
        }
        Set<String> distinct = new LinkedHashSet<>();
        for (String v : values) {
            if (distinct.size() < MAX_DISTINCT) distinct.add(v);
        }

        char decimal = sniffDecimal(values);
        int numbers = 0;
        int times = 0;
        int bools = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        for (String v : values) {
            double d = CsvValues.parseNumber(v, decimal);
            if (!Double.isNaN(d)) {
                numbers++;
                min = Math.min(min, d);
                max = Math.max(max, d);
            }
            if (CsvValues.parseTime(v) != null) times++;
            if (CsvValues.parseBool(v) != null) bools++;
        }

        int n = values.size();
        // 90%: ένα αρχείο έχει σχεδόν πάντα λίγες χαλασμένες τιμές, και δεν θέλουμε μια στήλη
        // 10.000 αριθμών να χαρακτηριστεί κείμενο εξαιτίας τριών.
        boolean mostly = n > 0;
        CsvValues.Type type;
        if (!mostly) {
            type = CsvValues.Type.TEXT;
        } else if (bools * 10 >= n * 9) {
            type = CsvValues.Type.BOOL;
        } else if (times * 10 >= n * 9 && looksTemporal(name, numbers == n, min, max)) {
            type = CsvValues.Type.TIMESTAMP;
        } else if (numbers * 10 >= n * 9) {
            type = CsvValues.Type.NUMBER;
        } else if (times * 10 >= n * 9) {
            type = CsvValues.Type.TIMESTAMP;
        } else {
            type = CsvValues.Type.TEXT;
        }

        String role = role(type, name, n, distinct.size(), timeTaken, keyTaken);
        List<String> samples = new ArrayList<>(distinct).subList(0, Math.min(4, distinct.size()));
        return new Column(index, name, type.name().toLowerCase(Locale.ROOT), String.valueOf(decimal),
                n, missing, distinct.size(),
                numbers > 0 ? min : null, numbers > 0 ? max : null,
                List.copyOf(samples), role);
    }

    /**
     * Ένα epoch είναι ταυτόχρονα αριθμός και χρόνος. Το κρίνουμε χρόνο μόνο αν το λέει το όνομα
     * της στήλης ή αν οι τιμές πέφτουν στο εύρος «αληθινή ημερομηνία»: αλλιώς μια στήλη με
     * πληθυσμούς πόλεων θα γινόταν ημερομηνίες του 1970 και το γράφημα θα ήταν άδειο.
     */
    private static boolean looksTemporal(String name, boolean allNumeric, double min, double max) {
        if (!allNumeric) {
            return true; // κείμενο που διαβάζεται ως ημερομηνία ΕΙΝΑΙ ημερομηνία
        }
        String n = name.toLowerCase(Locale.ROOT);
        boolean named = n.contains("time") || n.contains("date") || n.equals("ts") || n.endsWith("_ts")
                || n.contains("stamp") || n.contains("ώρα") || n.contains("ημερ") || n.contains("χρον");
        // 1e9 s = 2001, 1e12 ms = 2001· κάτω από αυτά ένας αριθμός δεν είναι ρεαλιστικό epoch
        boolean inEpochRange = (min >= 1.0e9 && max <= 4.0e9) || (min >= 1.0e12 && max <= 4.0e12);
        return named && inEpochRange;
    }

    /** Το δεκαδικό σύμβολο της στήλης, από τη θέση του: {@code 1.234,56} vs {@code 1,234.56}. */
    static char sniffDecimal(List<String> values) {
        for (String raw : values) {
            String v = raw.strip();
            if (!looksNumericish(v)) continue;
            int comma = v.lastIndexOf(',');
            if (comma < 0) continue;
            int dot = v.lastIndexOf('.');
            if (dot >= 0 && dot < comma) {
                return ','; // 1.234,56 — το κόμμα είναι το τελευταίο, άρα το δεκαδικό
            }
            if (dot < 0 && v.length() - comma - 1 != 3) {
                return ','; // 3,14 — μετά το κόμμα δεν ακολουθεί τριάδα χιλιάδων
            }
        }
        return '.';
    }

    private static boolean looksNumericish(String v) {
        boolean digit = false;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c >= '0' && c <= '9') {
                digit = true;
            } else if (c != '.' && c != ',' && c != '+' && c != '-' && c != 'e' && c != 'E' && c != ' ') {
                return false;
            }
        }
        return digit;
    }

    private static String role(CsvValues.Type type, String name, int n, int distinct,
                               boolean timeTaken, boolean keyTaken) {
        if (n == 0) {
            return "skip"; // στήλη ολόκληρη κενή
        }
        return switch (type) {
            case TIMESTAMP -> timeTaken ? "label" : "time";
            case NUMBER -> "metric";
            case BOOL -> "label";
            // Χαμηλή καρδινότητα = οντότητα (σταθμός, host, σύμβολο): ιδανικό κλειδί partition.
            // Μοναδικές τιμές (id) θα έδιναν ένα partition ανά γραμμή και καμία σειρά ανά οντότητα.
            case TEXT -> !keyTaken && distinct >= 2 && distinct <= Math.max(8, n / 5) ? "key" : "label";
        };
    }

    private static CsvPlan suggest(String fileName, char delimiter, boolean hasHeader, List<Column> columns) {
        String source = sourceIdFrom(fileName);
        List<CsvPlan.Column> chosen = new ArrayList<>();
        String time = null;
        String key = null;
        // Το δεκαδικό σύμβολο είναι του αρχείου, όχι της στήλης: κρατάμε ό,τι λέει η πλειοψηφία
        // των αριθμητικών στηλών, ώστε μία παράξενη στήλη να μη χαλάει τις υπόλοιπες.
        int commas = 0;
        for (Column c : columns) {
            if ("number".equals(c.type()) && ",".equals(c.decimal())) {
                commas++;
            }
        }
        long numeric = columns.stream().filter(c -> "number".equals(c.type())).count();
        String decimal = numeric > 0 && commas * 2 > numeric ? "," : ".";

        for (Column c : columns) {
            switch (c.role()) {
                case "time" -> {
                    time = c.name();
                    // Η στήλη χρόνου δεν γίνεται και πεδίο: ο χρόνος ζει στο event_time
                }
                case "metric" -> chosen.add(new CsvPlan.Column(c.name(), c.name(), "number"));
                case "key" -> {
                    key = c.name();
                    chosen.add(new CsvPlan.Column(c.name(), c.name(), "text"));
                }
                case "label" -> chosen.add(new CsvPlan.Column(c.name(), c.name(), c.type()));
                default -> { /* skip */ }
            }
        }
        return new CsvPlan(source, "ingest." + source, true, 6, label(delimiter), decimal, hasHeader,
                time, null, key, chosen, false);
    }

    /** Το topic ΠΡΕΠΕΙ να αρχίζει από «ingest.» για να το διαβάσει ο consumer (pattern). */
    static String sourceIdFrom(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "csv";
        }
        String base = fileName.replace('\\', '/');
        base = base.substring(base.lastIndexOf('/') + 1);
        int dot = base.lastIndexOf('.');
        if (dot > 0) {
            base = base.substring(0, dot);
        }
        StringBuilder sb = new StringBuilder(base.length());
        for (char c : base.toLowerCase(Locale.ROOT).toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                sb.append(c);
            } else if (!sb.isEmpty() && sb.charAt(sb.length() - 1) != '-') {
                sb.append('-');
            }
        }
        while (!sb.isEmpty() && sb.charAt(sb.length() - 1) == '-') {
            sb.setLength(sb.length() - 1);
        }
        return sb.isEmpty() ? "csv" : sb.toString();
    }

    /** Δύο στήλες με το ίδιο όνομα θα έγραφαν η μία πάνω στην άλλη μέσα στο {@code fields} map. */
    private static void dedupe(List<String> names) {
        Set<String> seen = new LinkedHashSet<>();
        for (int i = 0; i < names.size(); i++) {
            String n = names.get(i);
            if (!seen.add(n)) {
                int k = 2;
                while (!seen.add(n + "_" + k)) {
                    k++;
                }
                names.set(i, n + "_" + k);
            }
        }
    }

    private static List<String[]> read(String sample, char delimiter, int max) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (CsvReader r = new CsvReader(new StringReader(sample), delimiter)) {
            String[] row;
            while (rows.size() < max && (row = r.next()) != null) {
                if (!CsvReader.isBlank(row)) {
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    private static String label(char delimiter) {
        return delimiter == '\t' ? "\\t" : String.valueOf(delimiter);
    }
}
