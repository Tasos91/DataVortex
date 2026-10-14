package com.thesis.datavortex.analytics.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableCaching
public class AnalyticsConfig {

    /** Η ανάλυση είναι ακριβή (clustering) και το UI ανανεώνει συχνά: αξίζει cache. */
    @Value("${datavortex.cache.ttl-seconds:15}")
    private long ttlSeconds;

    /**
     * Virtual threads για τις παράλληλες κλήσεις προς το api-service. Είναι καθαρή αναμονή
     * δικτύου: δεν χρειάζεται pool με ακριβά threads, και τα 4 CPU μένουν ελεύθερα για την ανάλυση.
     */
    @Bean(destroyMethod = "close")
    public java.util.concurrent.ExecutorService apiExecutor() {
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public CacheManager cacheManager(MeterRegistry registry) {
        CaffeineCacheManager manager = new CaffeineCacheManager("analysis", "proxy");
        manager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .maximumSize(200)
                .recordStats());
        manager.getCacheNames().forEach(name -> {
            if (manager.getCache(name) instanceof CaffeineCache cache) {
                CaffeineCacheMetrics.monitor(registry, cache.getNativeCache(), name);
            }
        });
        return manager;
    }

    /*
     * ΔΕΝ χρησιμοποιούμε ShallowEtagHeaderFilter.
     *
     * Το filter κρατά όλη την απάντηση στη μνήμη για να υπολογίσει hash και, όπως φάνηκε με
     * μέτρηση, εμποδίζει τη συμπίεση του Tomcat: η ίδια απάντηση γύριζε 83 KB ασυμπίεστη με GET
     * ενώ με HEAD (όπου το filter δεν παρεμβαίνει) γύριζε gzip.
     *
     * Το ETag κερδίζει μόνο όταν η απάντηση ΔΕΝ έχει αλλάξει. Εδώ τα δεδομένα είναι χρονοσειρές
     * που ανανεώνονται συνεχώς, άρα σχεδόν κάθε απάντηση είναι διαφορετική: το gzip (−80% σε
     * ΚΑΘΕ απάντηση) αξίζει περισσότερο από ένα 304 που σπάνια θα συμβεί.
     */
}
