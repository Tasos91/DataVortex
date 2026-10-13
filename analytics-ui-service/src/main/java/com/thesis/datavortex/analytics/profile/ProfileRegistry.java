package com.thesis.datavortex.analytics.profile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Διαλέγει ποιο προφίλ θα αναλύσει μια πηγή.
 * <p>
 * Η επιλογή γίνεται με βαθμολογία και όχι με πίνακα αντιστοίχισης, για να δουλεύει και σε πηγές
 * που δεν έχει δει ποτέ κανείς: κάθε προφίλ κοιτάζει τα ονόματα των πεδίων και το όνομα της
 * πηγής και λέει πόσο ταιριάζει. Το γενικό προφίλ βαθμολογεί πάντα 1, άρα υπάρχει πάντα
 * απάντηση. Ο χρήστης μπορεί να επιβάλει προφίλ με παράμετρο.
 */
@Component
public class ProfileRegistry {

    private static final Logger log = LoggerFactory.getLogger(ProfileRegistry.class);

    private final List<DataProfile> profiles;

    public ProfileRegistry(List<DataProfile> profiles) {
        this.profiles = profiles;
        log.info("Διαθέσιμα προφίλ ανάλυσης: {}", profiles.stream().map(DataProfile::id).toList());
    }

    public List<DataProfile> all() {
        return profiles;
    }

    public Optional<DataProfile> byId(String id) {
        return profiles.stream().filter(p -> p.id().equalsIgnoreCase(id)).findFirst();
    }

    /**
     * @param forced προαιρετικό id προφίλ που επιβάλλει ο χρήστης
     */
    public DataProfile resolve(String sourceId, List<String> fields, String forced) {
        if (forced != null && !forced.isBlank() && !"auto".equalsIgnoreCase(forced)) {
            return byId(forced).orElseThrow(() ->
                    new IllegalArgumentException("Άγνωστο προφίλ '" + forced + "'. Διαθέσιμα: "
                            + profiles.stream().map(DataProfile::id).toList()));
        }
        Map<String, FieldRoles> roles = FieldRoles.of(fields);
        return profiles.stream()
                .max(Comparator.comparingInt(p -> p.score(sourceId, roles)))
                .orElseThrow(() -> new IllegalStateException("Δεν υπάρχει κανένα προφίλ ανάλυσης."));
    }
}
