package com.thesis.kafkacollector.format;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * «Πού μέσα στο έγγραφο είναι οι εγγραφές;»
 * <p>
 * Αυτό είναι το κομμάτι που κάνει τον σωλήνα να δέχεται <b>οποιαδήποτε</b> δομή. Μέχρι τώρα η
 * παραδοχή ήταν ότι το έγγραφο είναι είτε μία εγγραφή είτε μια λίστα εγγραφών. Στην πράξη τα
 * δεδομένα έρχονται θαμμένα:
 *
 * <pre>
 * {"status": "ok", "page": 3,
 *  "data": {"stations": [
 *      {"id": "ATH", "readings": [ {...}, {...} ]},
 *      {"id": "SKG", "readings": [ {...} ]}
 *  ]}}
 * </pre>
 *
 * Με {@code recordPath = "$.data.stations[*].readings[*]"} οι εγγραφές είναι οι τρεις μετρήσεις,
 * και όχι το ένα έγγραφο. Το {@code [*]} σημαίνει «για κάθε στοιχείο» και μπορεί να εμφανιστεί
 * <b>όσες φορές χρειαστεί</b>, σε οποιοδήποτε βάθος: η διαδρομή ξεδιπλώνεται αναδρομικά.
 * <p>
 * Τα πεδία μιας εγγραφής γράφονται μετά <b>σχετικά</b> ως προς αυτήν ({@code $.temp}), ενώ όσα
 * ζουν πιο έξω διαβάζονται με {@code $$} από τη ρίζα του εγγράφου ({@code $$.data.stations[0].id}
 * — ή, πιο χρήσιμα, μέσω των κληρονομούμενων πεδίων· δες {@code FieldSpec}).
 */
public final class RecordPath {

    /** Ένα βήμα: όνομα πεδίου, συγκεκριμένη θέση, ή «όλα τα στοιχεία». */
    private sealed interface Step {
        record Name(String name) implements Step {
        }

        record Index(int index) implements Step {
        }

        record Each() implements Step {
        }
    }

    private static final Step[] ROOT = new Step[0];

    private final Step[] steps;
    private final String source;

    private RecordPath(Step[] steps, String source) {
        this.steps = steps;
        this.source = source;
    }

    /** Η προεπιλογή: η ρίζα· αν είναι πίνακας, κάθε στοιχείο του είναι εγγραφή. */
    public static RecordPath root() {
        return new RecordPath(ROOT, "$");
    }

    /**
     * @param path π.χ. {@code $.data.items[*]}· κενό ή {@code null} σημαίνει η ρίζα
     * @throws IllegalArgumentException αν η σύνταξη είναι άκυρη — καλύτερα τώρα παρά σιωπηλά μηδέν εγγραφές
     */
    public static RecordPath of(String path) {
        if (path == null || path.isBlank() || "$".equals(path.trim())) {
            return root();
        }
        String p = path.trim();
        if (p.charAt(0) != '$') {
            // Σκέτο όνομα: κλειδί πρώτου επιπέδου, όπως και στα πεδία
            return new RecordPath(new Step[]{new Step.Name(p)}, p);
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
                    throw new IllegalArgumentException("Κενό όνομα στη διαδρομή εγγραφών: " + p);
                }
                steps.add(new Step.Name(p.substring(start, i)));
            } else if (c == '[') {
                int end = p.indexOf(']', i);
                if (end < 0) {
                    throw new IllegalArgumentException("Αγκύλη που δεν κλείνει στη διαδρομή: " + p);
                }
                String inside = p.substring(i + 1, end).trim();
                i = end + 1;
                if ("*".equals(inside)) {
                    steps.add(new Step.Each());
                } else if (inside.length() >= 2 && (inside.charAt(0) == '\'' || inside.charAt(0) == '"')
                        && inside.charAt(inside.length() - 1) == inside.charAt(0)) {
                    steps.add(new Step.Name(inside.substring(1, inside.length() - 1)));
                } else {
                    try {
                        steps.add(new Step.Index(Integer.parseInt(inside)));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Περίμενα αριθμό, '*' ή όνομα μέσα στις αγκύλες: " + p);
                    }
                }
            } else {
                throw new IllegalArgumentException("Άκυρος χαρακτήρας '" + c + "' στη διαδρομή: " + p);
            }
        }
        return new RecordPath(steps.toArray(new Step[0]), p);
    }

    /**
     * Καλεί το {@code action} για κάθε εγγραφή που βρίσκει.
     * <p>
     * Στη ρίζα χωρίς βήματα κρατιέται η παλιά, βολική συμπεριφορά: ένας πίνακας σημαίνει «κάθε
     * στοιχείο είναι εγγραφή», ένα αντικείμενο σημαίνει «μία εγγραφή».
     */
    public void forEach(JsonNode root, Consumer<JsonNode> action) {
        forEachWithScope(root, (record, scope) -> action.accept(record));
    }

    /**
     * Το ίδιο, αλλά με πρόσβαση και στους <b>προγόνους</b> κάθε εγγραφής.
     * <p>
     * Σε {@code $.stations[*].readings[*]} η θερμοκρασία ζει στη μέτρηση, αλλά το όνομα του
     * σταθμού ζει στον <b>γονέα</b>. Χωρίς αυτό, η πιο φυσική δομή δεδομένων του κόσμου —
     * «οντότητα με λίστα μετρήσεων» — θα έχανε την ταυτότητα της οντότητας, που είναι ακριβώς
     * αυτό που χρειάζεται για κλειδί partition και για ετικέτα.
     */
    public void forEachWithScope(JsonNode root, java.util.function.BiConsumer<JsonNode, Scope> action) {
        if (root == null || root.isMissingNode()) {
            return;
        }
        Deque<JsonNode> ancestors = new ArrayDeque<>(4);
        if (steps.length == 0) {
            Scope scope = new Scope(root, ancestors);
            if (root.isArray()) {
                root.forEach(child -> action.accept(child, scope));
            } else {
                action.accept(root, scope);
            }
            return;
        }
        walk(root, 0, ancestors, root, action);
    }

    /** Το περιβάλλον μιας εγγραφής: η ρίζα και η αλυσίδα των αντικειμένων που την περιέχουν. */
    public record Scope(JsonNode root, Deque<JsonNode> ancestors) {

        /**
         * @param levels 1 = ο γονέας, 2 = ο παππούς, κ.ο.κ.
         * @return το αντικείμενο τόσα επίπεδα πιο πάνω, ή {@code null} αν δεν υπάρχει
         */
        public JsonNode ancestor(int levels) {
            int i = 0;
            for (JsonNode node : ancestors) { // ArrayDeque: το πιο πρόσφατο πρώτο
                if (++i == levels) {
                    return node;
                }
            }
            return null;
        }
    }

    private void walk(JsonNode node, int stepIndex, Deque<JsonNode> ancestors, JsonNode root,
                      java.util.function.BiConsumer<JsonNode, Scope> action) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return;
        }
        if (stepIndex == steps.length) {
            Scope scope = new Scope(root, ancestors);
            // Φτάσαμε στο τέλος της διαδρομής: αν δείχνει σε πίνακα, κάθε στοιχείο είναι εγγραφή
            if (node.isArray()) {
                for (JsonNode child : node) {
                    action.accept(child, scope);
                }
            } else {
                action.accept(node, scope);
            }
            return;
        }
        switch (steps[stepIndex]) {
            case Step.Name(String name) -> walk(node.get(name), stepIndex + 1, ancestors, root, action);
            case Step.Index(int index) -> walk(node.get(index), stepIndex + 1, ancestors, root, action);
            case Step.Each() -> {
                // ΠΡΟΣΟΧΗ: το στοιχείο γίνεται πρόγονος μόνο αν ακολουθούν κι άλλα βήματα. Αν το
                // ξεδίπλωμα είναι το τελευταίο βήμα, το στοιχείο ΕΙΝΑΙ η εγγραφή — και μια εγγραφή
                // δεν είναι πρόγονος του εαυτού της (αλλιώς το «$^.x» θα έδειχνε στο «$.x»).
                boolean last = stepIndex + 1 == steps.length;
                if (node.isArray() || node.isObject()) {
                    // Σε αντικείμενο, «κάθε στοιχείο» σημαίνει οι τιμές του: δομές με δυναμικά
                    // κλειδιά, π.χ. {"ATH": {...}, "SKG": {...}}
                    for (JsonNode child : node) {
                        if (!last) {
                            ancestors.push(child);
                        }
                        walk(child, stepIndex + 1, ancestors, root, action);
                        if (!last) {
                            ancestors.pop();
                        }
                    }
                }
            }
        }
    }

    /** Δείχνει σε πολλές εγγραφές μέσα στο ίδιο έγγραφο; */
    public boolean isExploding() {
        for (Step step : steps) {
            if (step instanceof Step.Each) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return source;
    }
}
