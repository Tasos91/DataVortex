package com.thesis.kafkacollector.dto;

import java.util.Map;

public class IngestionRequest {
    private String sourceId = "1";
    private String fileType; // "CSV" ή "JSON"
    private String filePath;
    // Μέσα στο payload: το ίδιο το JSON που θα σταλεί (αντικείμενο ή πίνακας αντικειμένων).
    // Όταν υπάρχει, δεν χρειάζονται fileType / filePath.
    private Object payload;
    // Όνομα πεδίου Protobuf -> διαδρομή στο payload, e.g. {"EthPrice": "$.ethusdt.max"}
    // Για αρχεία CSV μπορεί να είναι και σκέτο όνομα στήλης: {"lat": "LATITUDE"}
    private Map<String, String> fieldMappings;

    public IngestionRequest() {}

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public String getFileType() { return fileType; }
    public void setFileType(String fileType) { this.fileType = fileType; }

    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }

    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    public Map<String, String> getFieldMappings() { return fieldMappings; }
    public void setFieldMappings(Map<String, String> fieldMappings) { this.fieldMappings = fieldMappings; }
}