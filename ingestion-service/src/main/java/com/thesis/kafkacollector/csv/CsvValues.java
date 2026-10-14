package com.thesis.kafkacollector.csv;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Το CSV δεν έχει τύπους: όλα είναι κείμενο. Εδώ γίνεται η μετατροπή σε τιμή με τύπο.
 * <p>
 * <b>Γιατί έχει σημασία:</b> πριν, κάθε στήλη CSV πήγαινε στο Protobuf ως
 * {@code string_value}. Ο writer γράφει στο {@code metric_points} μόνο ό,τι έχει
 * {@code jsonb_typeof = 'number'}, άρα <b>κανένα</b> CSV δεν παρήγαγε ποτέ μετρική: τα δεδομένα
 * έμπαιναν στη βάση αλλά δεν υπήρχαν ούτε χρονοσειρές ούτε ανάλυση. Ένα {@code "87.5"} και ένα
 * {@code 87.5} είναι η διαφορά ανάμεσα στο «αποθηκεύτηκε» και στο «μετρήθηκε».
 */
public final class CsvValues {

    private CsvValues() {}

    public enum Type {
        /** Αριθμός: γίνεται μετρική και μπαίνει στο {@code metric_points}. */
        NUMBER,
        /** Κείμενο: κατηγορία/ετικέτα. */
        TEXT,
        BOOL,
        /** Χρόνος: ο χρόνος του γεγονότος, ή epoch millis όταν ζητηθεί ως πεδίο. */
        TIMESTAMP
    }

    /**
     * Τιμές που σημαίνουν «δεν υπάρχει μέτρηση». ΔΕΝ γίνονται μηδέν: ένα μηδέν είναι μέτρηση και
     * σέρνει κάθε μέσο όρο προς τα κάτω. Η απουσία μένει απουσία και το πεδίο παραλείπεται.
     */
    private static final Set<String> MISSING = Set.of(
            "", "na", "n/a", "null", "nil", "none", "nan", "-", "--", "?", "ν/α");

    public static boolean isMissing(String s) {
        return s == null || MISSING.contains(s.strip().toLowerCase());
    }

    private static final Set<String> TRUE = Set.of("true", "yes", "y", "t", "ναι", "αληθές");
    private static final Set<String> FALSE = Set.of("false", "no", "n", "f", "όχι", "οχι", "ψευδές");

    public static Boolean parseBool(String s) {
        if (s == null) return null;
        String v = s.strip().toLowerCase();
        if (TRUE.contains(v)) return Boolean.TRUE;
        if (FALSE.contains(v)) return Boolean.FALSE;
        return null;
    }

    /**
     * Αριθμός με ανοχή στις μορφές που συναντάς αληθινά σε CSV: {@code 1.234,56} (ελληνικό/γερμανικό)
     * και {@code 1,234.56} (αγγλικό) διαβάζονται σωστά, αρκεί να ξέρουμε ποιο σύμβολο είναι το
     * δεκαδικό — αυτό το αποφασίζει ο {@link CsvInspector} για ΟΛΗ τη στήλη, όχι ανά τιμή, γιατί
     * το {@code 1,234} μόνο του είναι γνήσια αμφίσημο.
     *
     * @param decimal {@code '.'} ή {@code ','}· ο άλλος χαρακτήρας αγνοείται ως διαχωριστικό χιλιάδων
     * @return η τιμή, ή {@link Double#NaN} αν δεν είναι αριθμός
     */
    public static double parseNumber(String s, char decimal) {
        if (s == null) return Double.NaN;
        String v = s.strip();
        if (v.isEmpty()) return Double.NaN;

        char thousands = decimal == ',' ? '.' : ',';
        StringBuilder sb = new StringBuilder(v.length());
        boolean digit = false;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == thousands || c == '_' || c == ' ' || c == ' ') {
                continue; // διαχωριστικό χιλιάδων (και το αχώριστο κενό που βάζει το Excel)
            }
            if (c == decimal) {
                sb.append('.');
            } else if (c >= '0' && c <= '9') {
                sb.append(c);
                digit = true;
            } else if (c == '+' || c == '-' || c == 'e' || c == 'E') {
                sb.append(c);
            } else {
                return Double.NaN; // γράμματα, σύμβολα νομίσματος, ποσοστά: δεν είναι αριθμός
            }
        }
        if (!digit) return Double.NaN;
        try {
            return Double.parseDouble(sb.toString());
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    public static double parseNumber(String s) {
        return parseNumber(s, '.');
    }

    /**
     * Χρόνος από CSV, στις μορφές που εμφανίζονται στην πράξη. Επιστρέφει epoch millis.
     * <p>
     * Οι μορφές {@code dd/MM/yyyy} και {@code MM/dd/yyyy} είναι αμφίσημες και ΔΕΝ μαντεύονται:
     * μια λάθος εικασία μετακινεί δεδομένα μήνες μακριά, σιωπηλά. Για αυτές δίνεις ρητό
     * {@code timeFormat} στο plan.
     *
     * @return epoch millis, ή {@code null} αν δεν διαβάστηκε
     */
    public static Long parseTime(String s) {
        if (isMissing(s)) return null;
        String v = s.strip();

        // Σκέτος αριθμός: epoch. Κάτω από ~1e11 δεν μπορεί να είναι millis, άρα είναι δευτερόλεπτα.
        if (isAllDigits(v)) {
            try {
                long n = Long.parseLong(v);
                return n < 100_000_000_000L ? n * 1000L : n;
            } catch (NumberFormatException e) {
                return null;
            }
        }
        // Το κενό αντί για 'T' είναι η πιο συχνή μορφή σε CSV (SQL dumps, Excel)
        String iso = v.length() > 10 && v.charAt(10) == ' ' ? v.substring(0, 10) + 'T' + v.substring(11) : v;
        // Με ζώνη (Z ή offset) ο χρόνος είναι απόλυτος· χωρίς ζώνη τον διαβάζουμε ως UTC παρακάτω.
        if (iso.endsWith("Z") || iso.indexOf('+') > 10 || iso.lastIndexOf('-') > 10) {
            try {
                return Instant.parse(iso).toEpochMilli();
            } catch (DateTimeParseException ignored) {
                // πέφτουμε στις τοπικές μορφές
            }
        }
        try {
            return LocalDateTime.parse(iso).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException ignored) {
            // ούτε ημερομηνία-ώρα
        }
        try {
            return LocalDate.parse(iso).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    /**
     * Χρόνος με ρητό pattern (π.χ. {@code dd/MM/yyyy HH:mm}). Αν το pattern δεν έχει ώρα,
     * ο χρόνος είναι η αρχή της ημέρας σε UTC.
     */
    public static Long parseTime(String s, java.time.format.DateTimeFormatter fmt) {
        if (isMissing(s)) return null;
        String v = s.strip();
        try {
            return LocalDateTime.parse(v, fmt).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException e) {
            try {
                return LocalDate.parse(v, fmt).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    private static boolean isAllDigits(String v) {
        if (v.isEmpty()) return false;
        for (int i = 0; i < v.length(); i++) {
            if (v.charAt(i) < '0' || v.charAt(i) > '9') return false;
        }
        return true;
    }
}
