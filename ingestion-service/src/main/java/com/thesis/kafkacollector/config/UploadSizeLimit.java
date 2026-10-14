package com.thesis.kafkacollector.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Φράγμα μεγέθους στο {@code /api/v1/upload}.
 * <p>
 * Αυτό το endpoint δένει ΟΛΟ το σώμα στη μνήμη, και μάλιστα σε αρκετά αντίγραφα (Jackson bind →
 * JsonNode → JSON string ανά record). Ένα αρκετά μεγάλο request θα ρίξει τη διεργασία με
 * OutOfMemory — και μαζί της κάθε άλλη εισαγωγή που τρέχει εκείνη τη στιγμή. Αντί γι' αυτό,
 * απαντάμε 413 και λέμε πού να πάει ο χρήστης: το {@code /api/v1/stream} διαβάζει μία γραμμή τη
 * φορά και δεν έχει όριο.
 */
@Configuration
public class UploadSizeLimit {

    @Value("${datavortex.max-upload-bytes:33554432}") // 32 MB
    private long maxBytes;

    @Bean
    public FilterRegistrationBean<OncePerRequestFilter> uploadSizeLimitFilter() {
        OncePerRequestFilter filter = new OncePerRequestFilter() {
            @Override
            protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                            FilterChain chain) throws ServletException, IOException {
                long declared = request.getContentLengthLong();
                if (declared > maxBytes) {
                    response.setStatus(HttpStatus.PAYLOAD_TOO_LARGE.value());
                    response.setContentType(MediaType.TEXT_PLAIN_VALUE + ";charset=UTF-8");
                    response.getOutputStream().write(("""
                            Το σώμα είναι %d MB και το όριο για το /upload είναι %d MB, γιατί εδώ \
                            φορτώνεται ολόκληρο στη μνήμη.
                            Χρησιμοποίησε το POST /api/v1/stream με NDJSON: πρώτη γραμμή οι ρυθμίσεις, \
                            μετά μία εγγραφή ανά γραμμή. Διαβάζεται μία γραμμή τη φορά, χωρίς όριο μεγέθους.\
                            """).formatted(declared / (1024 * 1024), maxBytes / (1024 * 1024))
                            .getBytes(StandardCharsets.UTF_8));
                    return;
                }
                chain.doFilter(request, response);
            }
        };
        FilterRegistrationBean<OncePerRequestFilter> registration = new FilterRegistrationBean<>(filter);
        registration.addUrlPatterns("/api/v1/upload");
        return registration;
    }
}
