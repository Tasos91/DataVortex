package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.function.Consumer;

/**
 * Διαβάζει JSON σε <b>ροή</b> και παραδίδει μία εγγραφή τη φορά.
 * <p>
 * <b>Γιατί δεν φορτώνουμε απλώς το έγγραφο:</b> ένα αρχείο JSON 2 GB θα γινόταν δέντρο 6–10 GB
 * στη μνήμη και θα έριχνε τη διεργασία — μαζί με κάθε άλλη εισαγωγή που τρέχει εκείνη τη στιγμή.
 * <p>
 * Η λύση εκμεταλλεύεται ότι σχεδόν πάντα οι εγγραφές είναι ένας πίνακας κάπου μέσα στο έγγραφο
 * ({@code $.data.items[*]}). Ο αναγνώστης <b>πλοηγείται</b> ως εκεί χωρίς να χτίσει τίποτα —
 * προσπερνά ό,τι δεν τον αφορά — και από εκεί και πέρα χτίζει <b>ένα στοιχείο τη φορά</b>.
 * Έτσι η μνήμη εξαρτάται από το μέγεθος μιας εγγραφής, όχι του αρχείου.
 * <p>
 * Για διαδρομές που ξεδιπλώνονται σε πολλά επίπεδα ({@code $.a[*].b[*]}) η πλοήγηση δεν αρκεί και
 * το έγγραφο φορτώνεται· εκεί το {@link #isStreamable} επιστρέφει {@code false} και ο καλών το
 * ξέρει εκ των προτέρων.
 */
public final class JsonRecords {

    private JsonRecords() {
    }

    /**
     * Μπορεί αυτή η διαδρομή να διαβαστεί σε ροή;
     * <p>
     * Ναι όταν έχει το πολύ ένα «για κάθε στοιχείο», και αυτό είναι στο τέλος: τότε η πλοήγηση
     * είναι μια ευθεία κάθοδος και το ξεδίπλωμα γίνεται μία φορά, στο τελικό σημείο.
     */
    public static boolean isStreamable(String recordPath) {
        if (recordPath == null || recordPath.isBlank() || "$".equals(recordPath.trim())) {
            return true;
        }
        String p = recordPath.trim();
        int first = p.indexOf("[*]");
        return first < 0 || first == p.length() - 3;
    }

    /**
     * @param recordPath πού είναι οι εγγραφές· κενό σημαίνει η ρίζα (πίνακας = πολλές, αλλιώς μία)
     * @return πόσες εγγραφές διαβάστηκαν
     */
    public static int forEach(Reader in, String recordPath, ObjectMapper mapper, Consumer<JsonNode> action)
            throws IOException {
        String[] steps = steps(recordPath);
        try (JsonParser parser = mapper.getFactory().createParser(in)) {
            if (parser.nextToken() == null) {
                return 0;
            }
            for (String step : steps) {
                if (!descend(parser, step)) {
                    return 0; // η διαδρομή δεν υπάρχει μέσα στο έγγραφο
                }
            }
            return emit(parser, mapper, action);
        }
    }

    /** Τα ονόματα των βημάτων, χωρίς το τελικό «για κάθε στοιχείο» (που το κάνει το emit). */
    private static String[] steps(String recordPath) {
        if (recordPath == null || recordPath.isBlank() || "$".equals(recordPath.trim())) {
            return new String[0];
        }
        String p = recordPath.trim();
        if (p.endsWith("[*]")) {
            p = p.substring(0, p.length() - 3);
        }
        if (p.startsWith("$.")) {
            p = p.substring(2);
        } else if (p.startsWith("$")) {
            p = p.substring(1);
        }
        return p.isEmpty() ? new String[0] : p.split("\\.");
    }

    /**
     * Προχωρά τον parser μέσα στο πεδίο {@code name} του τρέχοντος αντικειμένου, προσπερνώντας
     * όσα δεν χρειάζονται. Ο parser μένει στο πρώτο token της τιμής.
     */
    private static boolean descend(JsonParser parser, String name) throws IOException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            return false;
        }
        while (parser.nextToken() == JsonToken.FIELD_NAME) {
            String field = parser.currentName();
            parser.nextToken(); // μετακίνηση στην τιμή
            if (name.equals(field)) {
                return true;
            }
            parser.skipChildren(); // ΕΔΩ είναι το κέρδος: ό,τι δεν μας αφορά δεν χτίζεται ποτέ
        }
        return false;
    }

    /** Από το τρέχον token: πίνακας σημαίνει μία εγγραφή ανά στοιχείο, αλλιώς μία εγγραφή. */
    private static int emit(JsonParser parser, ObjectMapper mapper, Consumer<JsonNode> action)
            throws IOException {
        int count = 0;
        if (parser.currentToken() == JsonToken.START_ARRAY) {
            while (parser.nextToken() != JsonToken.END_ARRAY && parser.currentToken() != null) {
                action.accept(mapper.readTree(parser));
                count++;
            }
        } else {
            action.accept(mapper.readTree(parser));
            count = 1;
        }
        return count;
    }
}
