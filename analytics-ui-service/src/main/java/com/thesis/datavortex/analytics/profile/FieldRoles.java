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
    UTILIZATION("util", "usage", "load", "cpu", "gpu", "busy", "percent"),
    LATENCY("latency", "delay", "duration", "resptime", "responsetime", "_ms", "ms", "lag", "wait"),
    ERRORS("error", "err", "fail", "drop", "reject", "retry", "timeout"),
    THROUGHPUT("throughput", "rate", "qps", "rps", "ops", "requests", "msgs", "count", "bytes"),
    MEMORY("mem", "ram", "heap", "swap"),
    TEMPERATURE("temp", "thermal", "celsius"),
    HUMIDITY("humid", "rh"),
    PRESSURE("pressure", "hpa", "mbar", "baro"),
    WIND("wind", "gust"),
    RAIN("rain", "precip", "snow", "mm"),
    OTHER();

    private final List<String> hints;

    FieldRoles(String... hints) {
        this.hints = List.of(hints);
    }

    public static FieldRoles of(String field) {
        String name = field.toLowerCase().replace("_", "").replace("-", "");
        for (FieldRoles role : values()) {
            for (String hint : role.hints) {
                if (name.contains(hint.replace("_", ""))) {
                    return role;
                }
            }
        }
        return OTHER;
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
