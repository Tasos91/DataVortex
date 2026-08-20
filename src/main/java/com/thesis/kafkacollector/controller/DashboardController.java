package com.thesis.kafkacollector.controller;
import com.thesis.kafkacollector.service.DashboardMetricsService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class DashboardController {

    private final DashboardMetricsService metricsService;

    public DashboardController(DashboardMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    // Το API που δίνει το JSON
    @GetMapping("/dashboard/api/stats")
    public Map<String, Object> getStats() {
        return Map.of(
                "totalMessages", metricsService.getTotalMessages(),
                "messagesPerSec", String.format("%.2f", metricsService.getMessagesPerSecond()),
                "lastMessage", metricsService.getLastMessage()
        );
    }

    // Το γραφικό περιβάλλον
    @GetMapping(value = "/dashboard", produces = MediaType.TEXT_HTML_VALUE)
    public String getDashboardUi() {
        return """
            <!DOCTYPE html>
            <html lang="el">
            <head>
                <meta charset="UTF-8">
                <title>Kafka Ingestion Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; background-color: #f4f4f9; padding: 20px; }
                    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-bottom: 20px; }
                    .metric { font-size: 2em; font-weight: bold; color: #007bff; }
                    .label { color: #555; font-size: 1.2em; }
                    pre { background: #333; color: #0f0; padding: 10px; border-radius: 5px; overflow-x: auto; }
                </style>
            </head>
            <body>
                <h1>🚀 Kafka Live Dashboard</h1>
                
                <div class="card">
                    <div class="label">Συνολικά Μηνύματα (Κατανάλωση)</div>
                    <div class="metric" id="totalMsg">0</div>
                </div>
                
                <div class="card">
                    <div class="label">Ταχύτητα (Μηνύματα / Δευτερόλεπτο)</div>
                    <div class="metric" id="speed">0.00 msg/s</div>
                </div>

                <div class="card">
                    <div class="label">Τελευταίο Payload</div>
                    <pre id="lastMsg">Περιμένω δεδομένα...</pre>
                </div>

                <script>
                    setInterval(() => {
                        fetch('/dashboard/api/stats')
                            .then(response => response.json())
                            .then(data => {
                                document.getElementById('totalMsg').innerText = data.totalMessages;
                                document.getElementById('speed').innerText = data.messagesPerSec + " msg/s";
                                document.getElementById('lastMsg').innerText = data.lastMessage || "Περιμένω δεδομένα...";
                            });
                    }, 1000);
                </script>
            </body>
            </html>
            """;
    }
}
