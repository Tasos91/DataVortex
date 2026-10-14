package com.thesis.kafkacollector.dto;

import java.util.Map;

public class IngestionRequest {
    private String sourceId = "1";
    private String fileType; // "CSV" ή "JSON"
    private String filePath;

    // Σε ποιο Kafka topic θα σταλούν τα μηνύματα. Αν λείπει, χρησιμοποιείται το default του application.yaml.
    private String topic;
    // Αν true και το topic δεν υπάρχει, δημιουργείται (με "partitions" partitions).
    private boolean createTopicIfMissing;
    private Integer partitions;

    // Διαδρομή στο payload για το κλειδί του μηνύματος, π.χ. "$.vessel.mmsi".
    // Το κλειδί καθορίζει το partition, άρα και τη σειρά και τον παραλληλισμό:
    // ίδιο κλειδί -> ίδιο partition -> εγγυημένη σειρά. Αν λείπει, τα μηνύματα
    // μοιράζονται σε όλα τα partitions (μέγιστο throughput, χωρίς εγγύηση σειράς).
    private String keyField;

    // Διαδρομή στο payload για τον χρόνο του γεγονότος, π.χ. "$.ts" (epoch millis ή ISO-8601).
    // Χωρίς αυτό, χρόνος είναι η στιγμή της αποστολής — δηλαδή ιστορικά δεδομένα θα
    // εμφανίζονταν όλα "τώρα" και δεν θα υπήρχε χρονοσειρά.
    private String timeField;

    // Μέσα στο payload: το ίδιο το JSON που θα σταλεί (αντικείμενο ή πίνακας αντικειμένων).
    // Όταν υπάρχει, δεν χρειάζονται fileType / filePath.
    private Object payload;
    // Να κρατηθεί ολόκληρο το αρχικό JSON μαζί με τα πεδία; Default true για συμβατότητα: το
    // /api/v1/records δείχνει τις ωμές εγγραφές. Με false γλιτώνεις τη μισή κίνηση προς το Kafka
    // και τον μισό χώρο στη βάση, αφού τα πεδία που διάλεξες αποθηκεύονται ούτως ή άλλως.
    private boolean storeRawPayload = true;

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

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public boolean isCreateTopicIfMissing() { return createTopicIfMissing; }
    public void setCreateTopicIfMissing(boolean createTopicIfMissing) { this.createTopicIfMissing = createTopicIfMissing; }

    public Integer getPartitions() { return partitions; }
    public void setPartitions(Integer partitions) { this.partitions = partitions; }

    public String getKeyField() { return keyField; }
    public void setKeyField(String keyField) { this.keyField = keyField; }

    public String getTimeField() { return timeField; }
    public void setTimeField(String timeField) { this.timeField = timeField; }

    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    public boolean isStoreRawPayload() { return storeRawPayload; }
    public void setStoreRawPayload(boolean storeRawPayload) { this.storeRawPayload = storeRawPayload; }

    public Map<String, String> getFieldMappings() { return fieldMappings; }
    public void setFieldMappings(Map<String, String> fieldMappings) { this.fieldMappings = fieldMappings; }
}
