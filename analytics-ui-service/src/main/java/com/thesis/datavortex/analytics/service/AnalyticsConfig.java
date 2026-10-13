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
import org.springframework.web.filter.ShallowEtagHeaderFilter;

import java.time.Duration;

@Configuration
@EnableCaching
public class AnalyticsConfig {

    /** Η ανάλυση είναι ακριβή (clustering) και το UI ανανεώνει συχνά: αξίζει cache. */
    @Value("${datavortex.cache.ttl-seconds:15}")
    private long ttlSeconds;

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
