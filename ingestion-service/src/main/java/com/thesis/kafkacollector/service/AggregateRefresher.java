package com.thesis.kafkacollector.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Υλοποιεί τα συγκεντρωτικά για δεδομένα που μπήκαν με <b>παλιό</b> χρόνο.
 * <p>
 * <b>Το bug που διορθώνει:</b> οι πολιτικές refresh καλύπτουν ένα κυλιόμενο παράθυρο γύρω από το
 * τώρα (3 ώρες για το {@code metric_1m}, 3 ημέρες για το {@code 1h}, 30 για το {@code 1d}). Αυτό
 * είναι σωστό για ζωντανή ροή, αλλά σημαίνει ότι ένα αρχείο με δεδομένα του περασμένου μήνα
 * <b>δεν υλοποιείται ποτέ</b>. Και το {@code materialized_only = false} δεν σώζει την κατάσταση:
 * ο real-time υπολογισμός καλύπτει μόνο ό,τι είναι <b>μετά</b> το watermark, όχι τρύπες πριν από
 * αυτό. Αποτέλεσμα: η εισαγωγή πετύχαινε, τα δεδομένα ήταν στη βάση, τα ερωτήματα γύρναγαν
 * <b>άδεια</b>, και κανένα σφάλμα δεν εμφανιζόταν πουθενά.
 * <p>
 * Εδώ κρατάμε το εύρος χρόνου που «λερώθηκε» από παλιές εγγραφές και το ανανεώνουμε μία φορά,
 * συγκεντρωτικά: το refresh είναι ακριβό και δεν έχει νόημα ανά batch. Η σειρά είναι ιεραρχική
 * ({@code 1m → 1h → 1d}) γιατί το κάθε επίπεδο διαβάζει από το προηγούμενο.
 */
@Component
public class AggregateRefresher {

    private static final Logger log = LoggerFactory.getLogger(AggregateRefresher.class);

    /** Ιεραρχική σειρά: το 1h διαβάζει από το 1m, το 1d από το 1h. Ανάποδα θα έβγαζε μισά δεδομένα. */
    private static final String[] VIEWS = {"metric_1m", "records_1m", "metric_1h", "metric_1d"};

    private final JdbcTemplate jdbc;
    private final Timer timer;
    private final Counter rounds;

    /** Τα όρια του εύρους που περιμένει ανανέωση· κενό όταν δεν υπάρχει τίποτα. */
    private final AtomicLong dirtyFrom = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong dirtyTo = new AtomicLong(Long.MIN_VALUE);
    /** Πότε ήρθε η τελευταία παλιά εγγραφή· από αυτό κρίνεται αν «ησύχασε» η εισαγωγή. */
    private final AtomicLong lastNoteAt = new AtomicLong();

    /**
     * Πόσο παλιά πρέπει να είναι μια εγγραφή για να θεωρηθεί backfill. Μέσα στο παράθυρο της
     * πολιτικής (3 ώρες) δεν χρειάζεται να κάνουμε τίποτα — θα το κάνει η πολιτική.
     */
    @Value("${datavortex.backfill-threshold-ms:7200000}")
    private long backfillThresholdMs;

    @Value("${datavortex.cagg-refresh-enabled:true}")
    private boolean enabled;

    /** Πόση ησυχία περιμένουμε πριν ανανεώσουμε, ώστε ένα αρχείο να κοστίσει ένα refresh. */
    @Value("${datavortex.cagg-quiet-ms:4000}")
    private long quietMs;

    public AggregateRefresher(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.timer = Timer.builder("datavortex.cagg.refresh")
                .description("Διάρκεια μιας ανανέωσης συγκεντρωτικών μετά από backfill")
                .register(registry);
        this.rounds = Counter.builder("datavortex.cagg.refresh.rounds")
                .description("Πόσες φορές χρειάστηκε ανανέωση για παλιά δεδομένα")
                .register(registry);
    }

    /**
     * Το δηλώνει ο writer μετά από κάθε batch. Κρατάμε μόνο τα όρια, όχι τα δεδομένα: ένα batch
     * 500 εγγραφών κοστίζει δύο συγκρίσεις.
     */
    public void noteWritten(long minEventTime, long maxEventTime) {
        if (!enabled || maxEventTime > System.currentTimeMillis() - backfillThresholdMs) {
            return; // ζωντανή ροή: η πολιτική refresh το καλύπτει
        }
        dirtyFrom.accumulateAndGet(minEventTime, Math::min);
        dirtyTo.accumulateAndGet(maxEventTime, Math::max);
        lastNoteAt.set(System.currentTimeMillis());
    }

    /**
     * Ανανεώνει όταν η εισαγωγή «ησυχάσει».
     * <p>
     * Το debounce δεν είναι λεπτομέρεια: το refresh σαρώνει όλα τα ωμά δεδομένα του εύρους και σε
     * μεγάλο όγκο κοστίζει δευτερόλεπτα ή λεπτά. Ένα αρχείο φτάνει στον consumer σε δεκάδες
     * batches· χωρίς την αναμονή θα πληρώναμε ένα refresh ανά batch, για το ίδιο εύρος.
     */
    @Scheduled(initialDelay = 5_000, fixedDelayString = "${datavortex.cagg-refresh-ms:5000}")
    public void refreshIfDirty() {
        if (!hasPending() || System.currentTimeMillis() - lastNoteAt.get() < quietMs) {
            return;
        }
        long from = dirtyFrom.getAndSet(Long.MAX_VALUE);
        long to = dirtyTo.getAndSet(Long.MIN_VALUE);
        if (from > to) {
            return;
        }
        refresh(from, to);
    }

    /**
     * Ανανέωση ενός ρητού εύρους, τώρα. Για δεδομένα που δεν πέρασαν από τον writer (επαναφορά
     * από backup, χειροκίνητο INSERT, ή εισαγωγές πριν υπάρξει αυτός ο μηχανισμός).
     */
    public void refreshNow(long from, long to) {
        refresh(from, to);
    }

    private void refresh(long from, long to) {
        // Στοίχιση στην ημέρα: το refresh_continuous_aggregate θέλει όρια πολλαπλάσια του bucket,
        // και η ημέρα είναι πολλαπλάσιο και του λεπτού και της ώρας.
        long day = TimeUnit.DAYS.toMillis(1);
        Timestamp start = new Timestamp(Math.floorDiv(from, day) * day);
        Timestamp end = new Timestamp((Math.floorDiv(to, day) + 1) * day);

        long began = System.nanoTime();
        try {
            for (String view : VIEWS) {
                // CALL και όχι SELECT: το refresh διαχειρίζεται δικά του transactions, γι' αυτό
                // δεν μπορεί να τρέξει μέσα σε ένα δικό μας.
                //
                // Το όνομα του view μπαίνει στο κείμενο του SQL και ΟΧΙ ως παράμετρος: το πρώτο
                // όρισμα είναι regclass και η Postgres δεν μπορεί να συμπεράνει τον τύπο μιας
                // παραμέτρου σε CALL ("could not determine data type of parameter $1"). Τα views
                // είναι σταθερές αυτής της κλάσης, άρα δεν υπάρχει είσοδος χρήστη εδώ.
                jdbc.update("CALL refresh_continuous_aggregate('" + view + "', ?::timestamptz, ?::timestamptz)",
                        start, end);
            }
            timer.record(System.nanoTime() - began, TimeUnit.NANOSECONDS);
            rounds.increment();
            log.info("Συγκεντρωτικά ανανεώθηκαν για {} → {} ({} ms)", start, end,
                    (System.nanoTime() - began) / 1_000_000);
        } catch (RuntimeException e) {
            // Ξαναβάζουμε το εύρος πίσω: η επόμενη προσπάθεια θα το πιάσει, και δεν χάνουμε
            // σιωπηλά την υλοποίηση — που ήταν ακριβώς το αρχικό bug.
            dirtyFrom.accumulateAndGet(from, Math::min);
            dirtyTo.accumulateAndGet(to, Math::max);
            log.warn("Η ανανέωση των συγκεντρωτικών απέτυχε, θα ξαναδοκιμαστεί: {}", e.getMessage());
        }
    }

    /** Υπάρχει εύρος που περιμένει ανανέωση; */
    public boolean hasPending() {
        return dirtyFrom.get() <= dirtyTo.get();
    }

    /** Ξεχνά ό,τι περιμένει, χωρίς να το ανανεώσει. Για να ξεκινά κάθε test από καθαρή κατάσταση. */
    void discardPending() {
        dirtyFrom.set(Long.MAX_VALUE);
        dirtyTo.set(Long.MIN_VALUE);
    }
}
