package com.thesis.kafkacollector.dto;

import java.util.Map;

public class IngestionRequest {
    private String sourceId = "1";
    private String fileType; // "CSV" ή "JSON"
    private String filePath;
    private Map<String, String> fieldMappings; // e.g. {"lat": "LATITUDE", "lon": "LONGITUDE"}

    public IngestionRequest() {}

    public String getSourceId() { return sourceId; }
    public void setSourceId(String sourceId) { this.sourceId = sourceId; }

    public String getFileType() { return fileType; }
    public void setFileType(String fileType) { this.fileType = fileType; }

    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }

    public Map<String, String> getFieldMappings() { return fieldMappings; }
    public void setFieldMappings(Map<String, String> fieldMappings) { this.fieldMappings = fieldMappings; }
}