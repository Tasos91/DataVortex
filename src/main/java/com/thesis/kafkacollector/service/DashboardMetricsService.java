package com.thesis.kafkacollector.service;

import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class DashboardMetricsService {

    private final AtomicLong totalMessages = new AtomicLong(0);
    private volatile long startTime = 0;
    private volatile String lastMessage = "";

    public void recordMessage(String message) {
        if (totalMessages.get() == 0) {
            startTime = System.currentTimeMillis();
        }
        totalMessages.incrementAndGet();
        this.lastMessage = message;
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public double getMessagesPerSecond() {
        long count = totalMessages.get();
        if (count == 0 || startTime == 0) return 0.0;

        long elapsedTimeMs = System.currentTimeMillis() - startTime;
        if (elapsedTimeMs == 0) return 0.0;

        return (count / (double) elapsedTimeMs) * 1000.0;
    }
}