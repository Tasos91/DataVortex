package com.thesis.kafkacollector.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Καθαρές διευθύνσεις για τις σελίδες: {@code /csv} αντί για {@code /csv.html}.
 * Η σελίδα μένει στατικό αρχείο (άρα cache-able και χωρίς template engine), αλλά η διεύθυνση που
 * δίνεις σε κάποιον είναι αυτή που θυμάται.
 */
@Configuration
public class WebPagesConfig implements WebMvcConfigurer {

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addViewController("/ingest").setViewName("forward:/ingest.html");
        // Η παλιά σελίδα μόνο για CSV παραμένει· η νέα καλύπτει κάθε μορφή.
        registry.addViewController("/csv").setViewName("forward:/csv.html");
    }
}
