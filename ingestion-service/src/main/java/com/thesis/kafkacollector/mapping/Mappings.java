package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Value;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Όλα όσα χρειάζεται μια εισαγωγή JSON, υπολογισμένα <b>πριν</b> την πρώτη εγγραφή: τα ονόματα των
 * πεδίων, οι μεταγλωττισμένες διαδρομές τους, η διαδρομή του κλειδιού και η διαδρομή του χρόνου.
 * <p>
 * Είναι το αντίστοιχο του {@code Layout} της διαδρομής CSV. Μέσα στον βρόχο των εγγραφών δεν
 * γίνεται καμία ανάλυση κειμένου, καμία αναζήτηση σε map και καμία δημιουργία συλλογής.
 */
public final class Mappings {

    private final String[] names;
    private final CompiledPath[] paths;
    private final CompiledPath keyPath;
    private final CompiledPath timePath;

    private Mappings(String[] names, CompiledPath[] paths, CompiledPath keyPath, CompiledPath timePath) {
        this.names = names;
        this.paths = paths;
        this.keyPath = keyPath;
        this.timePath = timePath;
    }

    /**
     * @param fieldMappings όνομα πεδίου → διαδρομή στο JSON· οι άκυρες διαδρομές παραλείπονται
     * @param keyField      διαδρομή για το κλειδί του μηνύματος, ή {@code null}
     * @param timeField     διαδρομή για τον χρόνο του γεγονότος, ή {@code null}
     */
    public static Mappings compile(Map<String, String> fieldMappings, String keyField, String timeField) {
        List<String> names = new ArrayList<>();
        List<CompiledPath> paths = new ArrayList<>();
        if (fieldMappings != null) {
            for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
                String name = entry.getKey();
                if (name == null || name.isBlank()) {
                    continue;
                }
                CompiledPath path = CompiledPath.of(entry.getValue());
                if (path != null) {
                    names.add(name);
                    paths.add(path);
                }
            }
        }
        return new Mappings(names.toArray(new String[0]), paths.toArray(new CompiledPath[0]),
                CompiledPath.of(keyField), CompiledPath.of(timeField));
    }

    public int size() {
        return names.length;
    }

    /**
     * Χτίζει το μήνυμα από μία εγγραφή JSON.
     *
     * @param payloadJson το ωμό κείμενο, ή {@code null} για να μην αποθηκευτεί (μισά bytes)
     */
    public IngestedRecord toRecord(String sourceId, JsonNode node, String payloadJson) {
        IngestedRecord.Builder builder = IngestedRecord.newBuilder()
                .setSourceId(sourceId != null ? sourceId : "");
        if (payloadJson != null) {
            builder.setPayloadJson(payloadJson);
        }
        for (int i = 0; i < paths.length; i++) {
            JsonNode value = paths[i].resolve(node);
            if (value != null) {
                builder.putFields(names[i], Values.of(value));
            }
        }
        return builder.build();
    }

    /** Το κλειδί του μηνύματος, ή {@code null} όταν δεν ορίστηκε ή δεν βρέθηκε. */
    public String keyOf(JsonNode node) {
        if (keyPath == null) {
            return null;
        }
        JsonNode value = keyPath.resolve(node);
        if (value == null || value.isNull()) {
            return null;
        }
        String key = value.isValueNode() ? value.asText() : value.toString();
        return key.isEmpty() ? null : key;
    }

    /** Ο χρόνος του γεγονότος σε epoch millis, ή {@code null} για «η ώρα της εισαγωγής». */
    public Long eventTimeOf(JsonNode node) {
        return timePath == null ? null : RecordMapper.toEpochMillis(timePath.resolve(node));
    }

    public boolean hasTimeField() {
        return timePath != null;
    }

    /** Για μηνύματα σφάλματος: ποιο πεδίο ζητήθηκε ως χρόνος. */
    public String timeFieldName() {
        return timePath == null ? null : timePath.toString();
    }

    /** Τα ονόματα των πεδίων που θα παραχθούν, με τη σειρά τους. */
    public List<String> names() {
        return List.of(names);
    }

    /** Βοηθητικό για τιμές που επαναλαμβάνονται· δες {@link Values}. */
    static Value value(JsonNode node) {
        return Values.of(node);
    }
}
