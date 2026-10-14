package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Μια διαδρομή JSON ({@code $.metrics.cpu}) αναλυμένη <b>μία φορά</b>, έτοιμη να εφαρμοστεί
 * εκατομμύρια φορές.
 * <p>
 * Ο {@link JsonPathResolver} αναλύει το κείμενο της διαδρομής σε κάθε κλήση: διαβάζει χαρακτήρα
 * προς χαρακτήρα, ψάχνει τελείες και αγκύλες, κόβει υποσυμβολοσειρές. Σωστό, αλλά το κείμενο της
 * διαδρομής είναι το <b>ίδιο</b> για όλες τις εγγραφές μιας εισαγωγής — με έξι πεδία και 200.000
 * εγγραφές γίνεται 1.200.000 φορές η ίδια ακριβώς ανάλυση, που παράγει πάντα το ίδιο αποτέλεσμα.
 * <p>
 * Εδώ η ανάλυση γίνεται μία φορά και μένει ένας πίνακας βημάτων. Η εφαρμογή της είναι μετά ένας
 * σκέτος βρόχος χωρίς καμία δημιουργία αντικειμένου.
 * <p>
 * Είναι το ίδιο μοτίβο με το {@code Layout} του CSV: ό,τι είναι σταθερό για όλη την εισαγωγή
 * υπολογίζεται πριν την πρώτη εγγραφή.
 */
public final class CompiledPath {

    /** Ένα βήμα: είτε όνομα πεδίου ({@code index < 0}) είτε θέση σε πίνακα. */
    private record Step(String name, int index) {
    }

    private static final Step[] WHOLE_DOCUMENT = new Step[0];

    private final Step[] steps;
    private final String source;

    private CompiledPath(Step[] steps, String source) {
        this.steps = steps;
        this.source = source;
    }

    /**
     * @return η μεταγλωττισμένη διαδρομή, ή {@code null} αν το κείμενο είναι κενό
     */
    public static CompiledPath of(String path) {
        if (path == null) {
            return null;
        }
        String p = path.trim();
        if (p.isEmpty()) {
            return null;
        }
        // Σκέτο όνομα χωρίς «$»: κλειδί πρώτου επιπέδου. Έτσι δούλευαν τα mappings για CSV, και
        // παραμένει έγκυρο — ένα όνομα στήλης είναι ακριβώς αυτό.
        if (p.charAt(0) != '$') {
            return new CompiledPath(new Step[]{new Step(p, -1)}, p);
        }

        List<Step> steps = new ArrayList<>(4);
        int i = 1;
        int n = p.length();
        while (i < n) {
            char c = p.charAt(i);
            if (c == '.') {
                int start = ++i;
                while (i < n && p.charAt(i) != '.' && p.charAt(i) != '[') {
                    i++;
                }
                if (start == i) {
                    return null; // «$..» ή σκέτο «$.» — άκυρη σύνταξη
                }
                steps.add(new Step(p.substring(start, i), -1));
            } else if (c == '[') {
                int end = p.indexOf(']', i);
                if (end < 0) {
                    return null;
                }
                String inside = p.substring(i + 1, end).trim();
                i = end + 1;
                if (inside.length() >= 2
                        && (inside.charAt(0) == '\'' || inside.charAt(0) == '"')
                        && inside.charAt(inside.length() - 1) == inside.charAt(0)) {
                    steps.add(new Step(inside.substring(1, inside.length() - 1), -1));
                } else {
                    try {
                        steps.add(new Step(null, Integer.parseInt(inside)));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
            } else {
                return null; // άκυρη σύνταξη
            }
        }
        return new CompiledPath(steps.isEmpty() ? WHOLE_DOCUMENT : steps.toArray(new Step[0]), p);
    }

    /**
     * @return ο κόμβος στη διαδρομή, ή {@code null} αν δεν υπάρχει
     */
    public JsonNode resolve(JsonNode root) {
        JsonNode current = root;
        for (Step step : steps) {
            if (current == null) {
                return null;
            }
            current = step.index() < 0 ? current.get(step.name()) : current.get(step.index());
        }
        return current == null || current.isMissingNode() ? null : current;
    }

    /**
     * Τα βήματα ως κείμενο: όνομα πεδίου, ή «[θέση]» για πίνακα. Τα χρησιμοποιεί το δέντρο κοινών
     * προθεμάτων, που διασχίζει μία φορά ό,τι μοιράζονται πολλά πεδία.
     */
    public String[] steps() {
        String[] out = new String[steps.length];
        for (int i = 0; i < steps.length; i++) {
            out[i] = steps[i].index() < 0 ? steps[i].name() : "[" + steps[i].index() + "]";
        }
        return out;
    }

    /** Το αρχικό κείμενο της διαδρομής, για μηνύματα σφάλματος. */
    @Override
    public String toString() {
        return source;
    }
}
