package com.thesis.datavortex.analytics.profile;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Πώς αναλύεται ΕΝΑΣ τύπος δεδομένων.
 * <p>
 * Δεν υπάρχει μία σωστή ανάλυση για όλα: η τηλεμετρία ενός GPU θέλει ομαδοποίηση σε καθεστώτα
 * φορτίου και κανόνα "υψηλή καθυστέρηση με χαμηλή χρήση = πρόβλημα", ενώ ο καιρός θέλει
 * εποχική βάση σύγκρισης (μια ζεστή νύχτα είναι ανωμαλία, ένα ζεστό μεσημέρι όχι) και
 * μετεωρολογικές κατηγορίες. Κάθε προφίλ κουβαλά τη δική του γνώση.
 * <p>
 * Νέος τύπος δεδομένων = νέα κλάση που υλοποιεί αυτό το interface και δηλώνεται ως bean.
 * Τίποτα άλλο δεν αλλάζει.
 */
public interface DataProfile {

    /** Σταθερό αναγνωριστικό, π.χ. {@code ai-telemetry}. */
    String id();

    /** Τίτλος για τον άνθρωπο. */
    String title();

    /** Σε μία φράση: τι κάνει διαφορετικά αυτό το προφίλ. */
    String approach();

    /**
     * Πόσο ταιριάζει σε αυτά τα δεδομένα.
     *
     * @return 0 = καθόλου· μεγαλύτερο κερδίζει. Το γενικό προφίλ επιστρέφει πάντα 1.
     */
    int score(String sourceId, Map<String, FieldRoles> roles);

    Analysis analyse(Input input);

    /**
     * Η είσοδος της ανάλυσης: ο πίνακας κουβάδες × πεδία, όπως τον δίνει το api-service.
     *
     * @param values ο ΜΕΣΟΣ ΟΡΟΣ ανά κουβά και πεδίο — δείχνει τη συμπεριφορά
     * @param peaks  η ΜΕΓΙΣΤΗ τιμή ανά κουβά και πεδίο — δείχνει τις αιχμές. Χρειάζονται και τα
     *               δύο: σε έναν κουβά με 80 μετρήσεις, μία αργή χάνεται τελείως στον μέσο όρο,
     *               άρα η ανίχνευση ανωμαλιών πάνω στον μέσο όρο δεν βρίσκει ποτέ τίποτα.
     */
    record Input(String source, String bucket, Instant from, Instant to,
                 List<String> fields, List<Instant> times,
                 List<Map<String, Double>> values, List<Map<String, Double>> peaks) {

        public int size() {
            return times.size();
        }

        /** Η στήλη ενός πεδίου (μέσοι όροι), με {@code NaN} όπου δεν υπάρχει μέτρηση. */
        public double[] column(String field) {
            return column(values, field);
        }

        /** Η στήλη των αιχμών ενός πεδίου· πέφτει στους μέσους όρους αν δεν υπάρχουν αιχμές. */
        public double[] peakColumn(String field) {
            return peaks == null || peaks.isEmpty() ? column(field) : column(peaks, field);
        }

        private double[] column(List<Map<String, Double>> source, String field) {
            double[] out = new double[times.size()];
            java.util.Arrays.fill(out, Double.NaN);
            for (int i = 0; i < times.size() && i < source.size(); i++) {
                Double v = source.get(i).get(field);
                if (v != null) {
                    out[i] = v;
                }
            }
            return out;
        }

        /**
         * Πίνακας μόνο με τα επιλεγμένα πεδία, σε πραγματικές μονάδες.
         * <p>
         * Οι αλγόριθμοι ομαδοποίησης δεν δέχονται κενά, οπότε ό,τι λείπει συμπληρώνεται με τη
         * <b>διάμεσο του πεδίου</b> — τιμή που δεν μετακινεί το κέντρο της ομάδας, σε αντίθεση
         * με το μηδέν που θα τραβούσε τεχνητά προς τα κάτω.
         */
        public double[][] matrix(List<String> selected) {
            double[][] out = new double[values.size()][selected.size()];
            for (int c = 0; c < selected.size(); c++) {
                double[] column = column(selected.get(c));
                double fallback = com.thesis.datavortex.analytics.ml.Stats.median(column);
                if (!Double.isFinite(fallback)) {
                    fallback = 0;
                }
                for (int r = 0; r < values.size(); r++) {
                    out[r][c] = Double.isFinite(column[r]) ? column[r] : fallback;
                }
            }
            return out;
        }
    }
}
