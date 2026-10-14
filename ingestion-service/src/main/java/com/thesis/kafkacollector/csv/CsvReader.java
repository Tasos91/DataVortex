package com.thesis.kafkacollector.csv;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Αναγνώστης CSV κατά RFC 4180, σε ροή: μία γραμμή στη μνήμη τη φορά.
 * <p>
 * Αντικαθιστά το {@code line.split(",")} που ήταν εδώ πριν και χάλαγε σε κάθε αληθινό αρχείο:
 * <ul>
 *   <li>{@code "Αθήνα, Ελλάδα",12} — το κόμμα μέσα σε εισαγωγικά ΔΕΝ χωρίζει πεδία·</li>
 *   <li>{@code "λέει ""γεια"""} — τα διπλά εισαγωγικά είναι ένα κυριολεκτικό {@code "}·</li>
 *   <li>αλλαγή γραμμής <b>μέσα</b> σε εισαγωγικά: μία λογική γραμμή σε δύο φυσικές·</li>
 *   <li>CRLF (Windows): χωρίς αυτό το τελευταίο πεδίο κάθε γραμμής κουβαλούσε {@code \r}·</li>
 *   <li>BOM στην αρχή: χωρίς αυτό η πρώτη στήλη λεγόταν {@code "﻿id"} και δεν έβρισκε mapping.</li>
 * </ul>
 * Δεν χρησιμοποιεί {@code split} ούτε regex· διαβάζει από δικό του buffer και φτιάχνει ένα
 * {@code String} ανά πεδίο — όσα αντικείμενα χρειάζονται και ούτε ένα παραπάνω.
 */
public final class CsvReader implements Closeable {

    /** Το μέγεθος του buffer· αρκετά μεγάλο για να μη γίνεται syscall ανά γραμμή. */
    private static final int BUF = 1 << 16;

    private final Reader in;
    private final char delimiter;
    private final char[] buf = new char[BUF];
    private int pos;
    private int limit;
    private boolean eof;

    private final StringBuilder field = new StringBuilder(64);
    private final List<String> row = new ArrayList<>(32);
    private long lineNumber;

    public CsvReader(Reader in, char delimiter) {
        this.in = in;
        this.delimiter = delimiter;
    }

    /** Η λογική γραμμή που μόλις διαβάστηκε (1 = η πρώτη). Για μηνύματα σφάλματος. */
    public long lineNumber() {
        return lineNumber;
    }

    /**
     * @return τα πεδία της επόμενης γραμμής, ή {@code null} στο τέλος της εισόδου.
     *         Ο πίνακας είναι καινούργιος κάθε φορά, άρα ο καλών μπορεί να τον κρατήσει.
     */
    public String[] next() throws IOException {
        row.clear();
        field.setLength(0);
        boolean inQuotes = false;
        boolean any = false;          // είδαμε έστω έναν χαρακτήρα ή διαχωριστικό
        boolean quotedField = false;  // το πεδίο ξεκίνησε με εισαγωγικό

        while (true) {
            int c = read();
            if (c < 0) {
                if (!any && field.isEmpty() && row.isEmpty()) {
                    return null; // καθαρό τέλος αρχείου
                }
                endField(quotedField);
                lineNumber++;
                return row.toArray(new String[0]);
            }
            char ch = (char) c;

            if (inQuotes) {
                if (ch == '"') {
                    // Διπλό εισαγωγικό = κυριολεκτικό ", αλλιώς κλείνει το πεδίο
                    int n = peek();
                    if (n == '"') {
                        read();
                        field.append('"');
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field.append(ch);
                }
                any = true;
                continue;
            }

            if (ch == '"' && field.isEmpty() && !quotedField) {
                inQuotes = true;
                quotedField = true;
                any = true;
            } else if (ch == delimiter) {
                endField(quotedField);
                quotedField = false;
                any = true;
            } else if (ch == '\n') {
                endField(quotedField);
                lineNumber++;
                return row.toArray(new String[0]);
            } else if (ch == '\r') {
                // CRLF ή σκέτο CR: και τα δύο τελειώνουν τη γραμμή
                if (peek() == '\n') {
                    read();
                }
                endField(quotedField);
                lineNumber++;
                return row.toArray(new String[0]);
            } else {
                field.append(ch);
                any = true;
            }
        }
    }

    /**
     * Κλείνει το τρέχον πεδίο. Τα κενά γύρω από μη-εισαγωγικό πεδίο φεύγουν (τα αρχεία
     * γράφονται συχνά με {@code ", "} μετά το κόμμα), αλλά ό,τι ήταν σε εισαγωγικά μένει
     * αυτούσιο: αν κάποιος έγραψε {@code "  x  "}, τα κενά είναι δεδομένο, όχι μορφοποίηση.
     */
    private void endField(boolean quoted) {
        row.add(quoted ? field.toString() : field.toString().strip());
        field.setLength(0);
    }

    /** Κενή γραμμή: ένα και μόνο κενό πεδίο. Προσπερνιέται αντί να γίνει εγγραφή με κενά. */
    public static boolean isBlank(String[] row) {
        return row.length == 0 || (row.length == 1 && row[0].isEmpty());
    }

    private int read() throws IOException {
        if (pos >= limit && !fill()) {
            return -1;
        }
        return buf[pos++];
    }

    private int peek() throws IOException {
        if (pos >= limit && !fill()) {
            return -1;
        }
        return buf[pos];
    }

    private boolean fill() throws IOException {
        if (eof) {
            return false;
        }
        int n = in.read(buf, 0, BUF);
        if (n <= 0) {
            eof = true;
            return false;
        }
        pos = 0;
        limit = n;
        return true;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    /** Αφαιρεί το BOM αν υπάρχει. Το πρώτο όνομα στήλης αλλιώς δεν ταιριάζει με κανένα mapping. */
    public static String stripBom(String s) {
        return s != null && !s.isEmpty() && s.charAt(0) == '﻿' ? s.substring(1) : s;
    }
}
