package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.thesis.kafkacollector.proto.IngestedRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Μετατρέπει ένα JSON record σε Protobuf {@link IngestedRecordProto.IngestedRecord}
 * με βάση τα field mappings που έδωσε ο χρήστης: {@code "ΟνομαΠεδίου": "$.διαδρομή.στο.json"}.
 * <p>
 * ΟΛΑ τα πεδία μπαίνουν στο δυναμικό map {@code fields}· κανένα όνομα δεν έχει ξεχωριστή
 * μεταχείριση, ώστε το σχήμα να μη ξέρει τίποτα για το νόημα των δεδομένων.
 */
public final class RecordMapper {

    private static final Logger log = LoggerFactory.getLogger(RecordMapper.class);

    private RecordMapper() {}

    public static IngestedRecordProto.IngestedRecord map(String sourceId,
                                                         String payloadJson,
                                                         JsonNode root,
                                                         Map<String, String> mappings) {
        IngestedRecordProto.IngestedRecord.Builder builder = IngestedRecordProto.IngestedRecord.newBuilder()
                .setSourceId(sourceId != null ? sourceId : "")
                .setPayloadJson(payloadJson != null ? payloadJson : "");

        if (mappings != null) {
            for (Map.Entry<String, String> mapping : mappings.entrySet()) {
                String name = mapping.getKey();
                if (name == null || name.isBlank()) continue;

                JsonNode node = JsonPathResolver.resolve(root, mapping.getValue());
                if (node == null) {
                    // debug και όχι warn: σε μεγάλο όγκο θα έβγαινε μία γραμμή ανά record
                    log.debug("Το mapping '{}' -> '{}' δεν βρήκε τιμή στο payload, το πεδίο παραλείπεται.",
                            name, mapping.getValue());
                    continue;
                }

                builder.putFields(name, Values.of(node));
            }
        }
        return builder.build();
    }

    /**
     * Βγάζει από το record το κλειδί του μηνύματος Kafka, με βάση τη διαδρομή {@code keyField}.
     *
     * @return το κλειδί ως String, ή {@code null} όταν δεν ορίστηκε διαδρομή ή δεν βρέθηκε τιμή
     *         (τότε το Kafka μοιράζει το μήνυμα σε όποιο partition θέλει)
     */
    public static String keyOf(JsonNode root, String keyField) {
        if (keyField == null || keyField.isBlank()) return null;
        JsonNode node = JsonPathResolver.resolve(root, keyField);
        if (node == null || node.isNull()) {
            log.debug("Το keyField '{}' δεν βρήκε τιμή· το μήνυμα φεύγει χωρίς κλειδί.", keyField);
            return null;
        }
        return node.isValueNode() ? node.asText() : node.toString();
    }

    /**
     * Ο χρόνος του γεγονότος από το payload: epoch millis (αριθμός) ή ISO-8601 (κείμενο).
     *
     * @return epoch millis, ή {@code null} αν δεν ορίστηκε/δεν διαβάστηκε — τότε χρόνος
     *         είναι η στιγμή της αποστολής
     */
    public static Long eventTimeOf(JsonNode root, String timeField) {
        if (timeField == null || timeField.isBlank()) return null;
        return toEpochMillis(JsonPathResolver.resolve(root, timeField));
    }

    /**
     * Ο χρόνος ενός κόμβου σε epoch millis: αριθμός (δευτερόλεπτα ή millis) ή κείμενο ISO-8601.
     *
     * @return epoch millis, ή {@code null} αν ο κόμβος λείπει ή δεν διαβάζεται ως χρόνος
     */
    public static Long toEpochMillis(JsonNode node) {
        if (node == null || node.isNull()) return null;
        try {
            if (node.isNumber()) {
                long v = node.asLong();
                // Δέχεται και δευτερόλεπτα: κάτω από ~1e11 δεν μπορεί να είναι millis εποχής
                return v < 100_000_000_000L ? v * 1000L : v;
            }
            String text = node.asText().trim();
            if (text.isEmpty()) return null;
            if (text.chars().allMatch(Character::isDigit)) {
                long v = Long.parseLong(text);
                return v < 100_000_000_000L ? v * 1000L : v;
            }
            return java.time.Instant.parse(text.endsWith("Z") || text.contains("+") ? text : text + "Z")
                    .toEpochMilli();
        } catch (RuntimeException e) {
            log.debug("Η τιμή '{}' δεν διαβάστηκε ως χρόνος: {}", node, e.getMessage());
            return null;
        }
    }

    /** Διατηρείται για τα tests του σχήματος· η παραγωγική διαδρομή περνά από το {@link Values}. */
    static Value toValue(JsonNode node) {
        return Values.of(node);
    }
}
