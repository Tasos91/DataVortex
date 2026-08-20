package com.thesis.kafkacollector.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "ingested_records")
public class IngestedRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "source_id")
    private Integer sourceId;

    @Column(columnDefinition = "jsonb", name = "payload")
    private String payload;

    private Double latitude;
    private Double longitude;

    @Column(name = "ingested_at")
    private LocalDateTime ingestedAt = LocalDateTime.now();

    public IngestedRecord() {}

    public IngestedRecord(Integer sourceId, String payload, Double latitude, Double longitude) {
        this.sourceId = sourceId;
        this.payload = payload;
        this.latitude = latitude;
        this.longitude = longitude;
        this.ingestedAt = LocalDateTime.now();
    }

    // Getters & Setters
    public Long getId() { return id; }
    public Integer getSourceId() { return sourceId; }
    public String getPayload() { return payload; }
    public Double getLatitude() { return latitude; }
    public Double getLongitude() { return longitude; }
}