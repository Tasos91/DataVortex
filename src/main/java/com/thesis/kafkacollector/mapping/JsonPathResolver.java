package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Μικρός resolver διαδρομών JSON χωρίς εξωτερική βιβλιοθήκη.
 * <p>
 * Υποστηρίζει:
 * <ul>
 *   <li>{@code $.ethusdt.max}        – εμφωλευμένα αντικείμενα</li>
 *   <li>{@code $.items[0].price}     – θέση σε πίνακα</li>
 *   <li>{@code $['my key'].max}      – κλειδιά με κενά ή τελείες</li>
 *   <li>{@code $}                    – ολόκληρο το payload</li>
 *   <li>{@code LATITUDE}             – σκέτο όνομα = κλειδί πρώτου επιπέδου (όπως παλιά, π.χ. για CSV)</li>
 * </ul>
 * Αν η διαδρομή δεν βρεθεί επιστρέφεται {@code null}.
 */
public final class JsonPathResolver {

    private JsonPathResolver() {}

    public static JsonNode resolve(JsonNode root, String path) {
        if (root == null || path == null) return null;
        String p = path.trim();
        if (p.isEmpty()) return null;

        // Συμβατότητα: σκέτο όνομα στήλης/κλειδιού, όπως δούλευε μέχρι τώρα
        if (p.charAt(0) != '$') {
            JsonNode direct = root.get(p);
            if (direct != null) return direct;
            p = "$." + p;
        }

        JsonNode current = root;
        int i = 1; // μετά το '$'
        int n = p.length();
        while (i < n && current != null) {
            char c = p.charAt(i);
            if (c == '.') {
                int start = ++i;
                while (i < n && p.charAt(i) != '.' && p.charAt(i) != '[') i++;
                if (start == i) return null;
                current = current.get(p.substring(start, i));
            } else if (c == '[') {
                int end = p.indexOf(']', i);
                if (end < 0) return null;
                String inside = p.substring(i + 1, end).trim();
                i = end + 1;
                if (inside.length() >= 2
                        && (inside.charAt(0) == '\'' || inside.charAt(0) == '"')
                        && inside.charAt(inside.length() - 1) == inside.charAt(0)) {
                    current = current.get(inside.substring(1, inside.length() - 1));
                } else {
                    try {
                        current = current.get(Integer.parseInt(inside));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }
            } else {
                return null; // άκυρη σύνταξη
            }
        }
        if (current == null || current.isMissingNode()) return null;
        return current;
    }
}
