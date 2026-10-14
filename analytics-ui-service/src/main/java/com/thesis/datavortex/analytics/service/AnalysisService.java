package com.thesis.datavortex.analytics.service;

import com.thesis.datavortex.analytics.client.ApiClient;
import com.thesis.datavortex.analytics.client.ApiDtos;
import com.thesis.datavortex.analytics.profile.Analysis;
import com.thesis.datavortex.analytics.profile.DataProfile;
import com.thesis.datavortex.analytics.profile.ProfileRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Συνδέει τα κομμάτια: παίρνει τον πίνακα από το api-service, διαλέγει προφίλ, τρέχει την ανάλυση.
 * <p>
 * Το clustering τρέχει πάνω σε <b>χρονικούς κουβάδες</b> και όχι σε ωμές γραμμές. Αυτή είναι η
 * απάντηση στο «να είναι γρήγορο όσο κι αν μεγαλώσουν τα δεδομένα»: 10 εκατομμύρια εγγραφές σε
 * 24 ώρες δίνουν τους ίδιους ~600 κουβάδες με 10 χιλιάδες, άρα ο χρόνος της ανάλυσης δεν
 * εξαρτάται από τον όγκο — μόνο από το εύρος που ζητήθηκε.
 */
@Service
public class AnalysisService {

    private static final Logger log = LoggerFactory.getLogger(AnalysisService.class);

    private final ApiClient api;
    private final ProfileRegistry registry;
    private final java.util.concurrent.ExecutorService executor;
    private final Timer analysisTimer;

    public AnalysisService(ApiClient api, ProfileRegistry registry, MeterRegistry meters,
                           java.util.concurrent.ExecutorService executor) {
        this.api = api;
        this.registry = registry;
        this.executor = executor;
        this.analysisTimer = Timer.builder("datavortex.analysis.duration")
                .description("Χρόνος μιας πλήρους ανάλυσης (clustering, κατάταξη, ανωμαλίες)")
                .publishPercentileHistogram()
                .register(meters);
    }

    @Cacheable(value = "analysis", key = "#source + '|' + #from + '|' + #to + '|' + #bucket + '|' + #profileId")
    public Analysis analyse(String source, Instant from, Instant to, String bucket, String profileId) {
        return analysisTimer.record(() -> {
            // Μέσοι όροι και αιχμές ταυτόχρονα: είναι δύο ανεξάρτητα requests σε άλλο service,
            // άρα ο συνολικός χρόνος είναι του πιο αργού και όχι το άθροισμα.
            var peaksFuture = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> api.matrix(source, from, to, bucket, "max"), executor);
            ApiDtos.Matrix matrix = api.matrix(source, from, to, bucket, "avg");
            if (matrix == null || matrix.rows() == null || matrix.rows().isEmpty()) {
                peaksFuture.cancel(true);
                DataProfile fallback = registry.resolve(source, List.of(), profileId);
                return emptyAnalysis(fallback, source, bucket, from, to);
            }

            List<Instant> times = new ArrayList<>(matrix.rows().size());
            List<Map<String, Double>> values = new ArrayList<>(matrix.rows().size());
            for (ApiDtos.Matrix.Row row : matrix.rows()) {
                times.add(row.t());
                values.add(row.values() == null ? Map.of() : row.values());
            }

            // Οι αιχμές ήρθαν παράλληλα· ο μέσος όρος ενός κουβά κρύβει τη μία αργή μέτρηση
            // μέσα στις 80 γρήγορες.
            List<Map<String, Double>> peaks = new ArrayList<>(times.size());
            try {
                ApiDtos.Matrix maxMatrix = peaksFuture.join();
                if (maxMatrix != null && maxMatrix.rows() != null
                        && maxMatrix.rows().size() == matrix.rows().size()) {
                    for (ApiDtos.Matrix.Row row : maxMatrix.rows()) {
                        peaks.add(row.values() == null ? Map.of() : row.values());
                    }
                }
            } catch (RuntimeException e) {
                log.debug("Οι αιχμές δεν ήρθαν, συνεχίζουμε με μέσους όρους: {}", e.getMessage());
            }

            DataProfile profile = registry.resolve(source, matrix.fields(), profileId);
            log.debug("Ανάλυση {} με προφίλ {} σε {} κουβάδες", source, profile.id(), times.size());
            return profile.analyse(new DataProfile.Input(source, matrix.bucket(),
                    matrix.from() != null ? matrix.from() : from,
                    matrix.to() != null ? matrix.to() : to,
                    matrix.fields(), times, values, peaks));
        });
    }

    private static Analysis emptyAnalysis(DataProfile profile, String source, String bucket,
                                          Instant from, Instant to) {
        return new Analysis(source, profile.id(), profile.title(), bucket, from, to, 0,
                List.of(), "—", List.of(), List.of(), List.of(), List.of(),
                new Analysis.Verdict(0, "NO_DATA", "Δεν υπάρχουν δεδομένα σε αυτό το διάστημα",
                        List.of("Δοκίμασε μεγαλύτερο εύρος ή άλλη πηγή.")), 0, 0);
    }
}
