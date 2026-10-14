package com.thesis.datavortex.analytics.profile;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Τι σημαίνει κάθε πεδίο, μαντεμένο από το όνομά του.
 * <p>
 * Τα πεδία τα ορίζει ο χρήστης στα fieldMappings, άρα το σύστημα δεν ξέρει εκ των προτέρων
 * ποιο είναι καθυστέρηση και ποιο θερμοκρασία. Χωρίς αυτήν τη γνώση, όλα τα πεδία είναι
 * "κάποιος αριθμός" και η ανάλυση δεν μπορεί να πει κάτι ουσιαστικό — π.χ. ότι υψηλή
 * καθυστέρηση με χαμηλή χρήση σημαίνει πρόβλημα.
 * <p>
 * Είναι ευρετική και μπορεί να πέσει έξω· γι' αυτό καμία ανάλυση δεν ΑΠΑΙΤΕΙ ρόλο, απλώς
 * λέει πιο συγκεκριμένα πράγματα όταν τον βρει.
 */
public enum FieldRoles {

    // ΠΡΟΣΟΧΗ στη σειρά: κερδίζει ο πρώτος ρόλος που ταιριάζει, άρα οι πιο ειδικοί πάνε πρώτοι.
    // Π.χ. το "error_count" είναι σφάλματα και όχι ρυθμός, παρότι περιέχει "count".
    // Τα ελληνικά hints γράφονται ΧΩΡΙΣ τόνους: το όνομα του πεδίου κανονικοποιείται πρώτα, ώστε
    // "Θερμοκρασία", "ΘΕΡΜΟΚΡΑΣΙΑ" και "θερμοκρασια" να είναι το ίδιο πράγμα.
    UTILIZATION("util", "usage", "load", "cpu", "gpu", "busy", "percent", "χρησ", "φορτι", "απασχολ"),
    LATENCY("latency", "delay", "duration", "resptime", "responsetime", "ms", "lag", "wait",
            "καθυστερ", "αναμον", "διαρκει"),
    ERRORS("error", "err", "fail", "drop", "reject", "retry", "timeout", "σφαλμ", "αποτυχ", "απορρι"),
    THROUGHPUT("throughput", "rate", "qps", "rps", "ops", "requests", "msgs", "count", "bytes",
            "ρυθμ", "αιτημ", "πληθ", "μηνυμ"),
    MEMORY("mem", "ram", "heap", "swap", "μνημ"),
    TEMPERATURE("temp", "thermal", "celsius", "θερμ"),
    HUMIDITY("humid", "rh", "υγρασ"),
    PRESSURE("pressure", "hpa", "mbar", "baro", "πιεσ"),
    WIND("wind", "gust", "ανεμ", "ριπ"),
    RAIN("rain", "precip", "snow", "mm", "βροχ", "χιον", "υετ"),
    OTHER();

    private final List<String> hints;

    FieldRoles(String... hints) {
        this.hints = List.of(hints);
    }

    public static FieldRoles of(String field) {
        String name = normalize(field);
        for (FieldRoles role : values()) {
            for (String hint : role.hints) {
                if (matches(name, hint)) {
                    return role;
                }
            }
        }
        return OTHER;
    }

    /**
     * Ένα hint δύο χαρακτήρων ταιριάζει μόνο στο τέλος ή ως ολόκληρο όνομα. Αλλιώς το "ms" της
     * καθυστέρησης χαρακτήριζε το "msgs" ως καθυστέρηση (και κέρδιζε, αφού το LATENCY είναι πριν
     * από το THROUGHPUT) και το "rh" της υγρασίας θα έπιανε κάθε "throughput".
     */
    private static boolean matches(String name, String hint) {
        return hint.length() > 2 ? name.contains(hint) : name.endsWith(hint) || name.equals(hint);
    }

    /**
     * Πεζά, χωρίς τόνους, χωρίς διαχωριστικά. Τα ελληνικά ονόματα πεδίων έρχονται και με τόνο και
     * χωρίς ("Θερμοκρασία" από CSV, "ΘΕΡΜΟΚΡΑΣΙΑ" από κεφαλαία επικεφαλίδα), και το τελικό «ς»
     * είναι άλλος χαρακτήρας από το «σ».
     */
    private static String normalize(String field) {
        String lower = field.toLowerCase(java.util.Locale.ROOT).replace("_", "").replace("-", "")
                .replace("ς", "σ");
        String decomposed = java.text.Normalizer.normalize(lower, java.text.Normalizer.Form.NFD);
        StringBuilder sb = new StringBuilder(decomposed.length());
        for (int i = 0; i < decomposed.length(); i++) {
            char c = decomposed.charAt(i);
            if (Character.getType(c) != Character.NON_SPACING_MARK) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static Map<String, FieldRoles> of(List<String> fields) {
        Map<String, FieldRoles> roles = new LinkedHashMap<>();
        for (String field : fields) {
            roles.put(field, of(field));
        }
        return roles;
    }

    /** Το πρώτο πεδίο με αυτόν τον ρόλο, αν υπάρχει. */
    public static Optional<String> first(Map<String, FieldRoles> roles, FieldRoles wanted) {
        return roles.entrySet().stream()
                .filter(e -> e.getValue() == wanted)
                .map(Map.Entry::getKey)
                .findFirst();
    }

    public static boolean hasAll(Map<String, FieldRoles> roles, FieldRoles... wanted) {
        for (FieldRoles role : wanted) {
            if (!roles.containsValue(role)) {
                return false;
            }
        }
        return true;
    }

    public static int countOf(Map<String, FieldRoles> roles, FieldRoles... wanted) {
        int found = 0;
        for (FieldRoles role : wanted) {
            if (roles.containsValue(role)) {
                found++;
            }
        }
        return found;
    }
}
