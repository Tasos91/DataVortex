package com.thesis.datavortex.api.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.ShallowEtagHeaderFilter;

import java.time.Duration;

/**
 * Δύο στρώσεις προστασίας της βάσης:
 * <ol>
 *   <li><b>cache</b> — το UI ρωτά τα ίδια συγκεντρωτικά κάθε λίγα δευτερόλεπτα· με μικρό TTL
 *       τα επαναλαμβανόμενα requests δεν φτάνουν ποτέ στη βάση·</li>
 *   <li><b>ETag</b> — όταν η απάντηση δεν άλλαξε, ο browser παίρνει 304 χωρίς σώμα.</li>
 * </ol>
 */
@Configuration
@EnableCaching
public class ApiConfig {

    /** Μικρό TTL: τα δεδομένα είναι χρονοσειρές και ανανεώνονται συνεχώς. */
    @Value("${datavortex.cache.ttl-seconds:10}")
    private long ttlSeconds;

    @Value("${datavortex.cache.max-entries:500}")
    private long maxEntries;

    @Bean
    public CacheManager cacheManager(MeterRegistry registry) {
        CaffeineCacheManager manager = new CaffeineCacheManager("series", "matrix", "rates", "meta");
        manager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .maximumSize(maxEntries)
                .recordStats());
        // Τα hit/miss φαίνονται στο Grafana: χωρίς μέτρηση, το cache είναι εικασία.
        manager.getCacheNames().forEach(name -> {
            var cache = manager.getCache(name);
            if (cache instanceof org.springframework.cache.caffeine.CaffeineCache caffeine) {
                CaffeineCacheMetrics.monitor(registry, caffeine.getNativeCache(), name);
            }
        });
        return manager;
    }

    /**
     * ETag μόνο στα API: το filter κρατά την απάντηση στη μνήμη για να υπολογίσει hash, και
     * σε στατικά αρχεία αυτό χαλάει την απευθείας αποστολή (η welcome page γύριζε κενή).
     * Τα στατικά έχουν άλλωστε ήδη Last-Modified.
     */
    @Bean
    public org.springframework.boot.web.servlet.FilterRegistrationBean<ShallowEtagHeaderFilter> etagFilter() {
        var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<>(
                new ShallowEtagHeaderFilter());
        registration.addUrlPatterns("/api/*");
        return registration;
    }
}
