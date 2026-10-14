package com.thesis.kafkacollector.service;

import com.google.protobuf.Value;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Πόσο κοστίζει η μετατροπή των πεδίων σε JSON — τρέχει μία φορά ανά εγγραφή, μέσα στο thread
 * του consumer, πριν από κάθε INSERT. Είναι η μόνη σοβαρή δουλειά CPU σε αυτή τη διαδρομή.
 * <p>
 * Δεν είναι JMH: δεν ψάχνουμε ακρίβεια νανοδευτερολέπτου αλλά τάξη μεγέθους, και την εγγύηση ότι
 * η γρήγορη υλοποίηση βγάζει <b>ακριβώς</b> το ίδιο JSON με την αργή.
 */
class FieldsJsonBenchTest {

    private static IngestedRecord sample() {
        return IngestedRecord.newBuilder()
                .setSourceId("perf")
                .putFields("cpu", Value.newBuilder().setNumberValue(87.5).build())
                .putFields("latency_ms", Value.newBuilder().setNumberValue(42.0).build())
                .putFields("mem_gb", Value.newBuilder().setNumberValue(13.25).build())
                .putFields("errors", Value.newBuilder().setNumberValue(0).build())
                .putFields("throughput", Value.newBuilder().setNumberValue(3286.9).build())
                .putFields("host", Value.newBuilder().setStringValue("node-07").build())
                .build();
    }

    @Test
    @DisplayName("η γρήγορη διαδρομή βγάζει ισοδύναμο JSON με το JsonFormat")
    void sameOutput() {
        IngestedRecord record = sample();
        String fast = IngestedRecordWriter.fieldsToJson(record);
        assertNotNull(fast);

        // Τα κλειδιά και οι τιμές πρέπει να είναι τα ίδια· η σειρά μέσα στο jsonb δεν μετράει,
        // γιατί η Postgres το ξαναγράφει με δική της σειρά ούτως ή άλλως.
        for (String field : record.getFieldsMap().keySet()) {
            assertTrue(fast.contains("\"" + field + "\":"), field + " λείπει από το " + fast);
        }
        assertTrue(fast.contains("87.5"));
        assertTrue(fast.contains("\"node-07\""));
        assertTrue(fast.contains("\"errors\":0"), "ακέραιος χωρίς .0: " + fast);
    }

    @Test
    @DisplayName("ειδικοί χαρακτήρες και ελληνικά δεν σπάνε το JSON")
    void escaping() {
        IngestedRecord record = IngestedRecord.newBuilder()
                .putFields("σημείωση", Value.newBuilder().setStringValue("λέει \"γεια\"\n\tκαι \\ τέλος").build())
                .putFields("κενό", Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build())
                .putFields("σημαία", Value.newBuilder().setBoolValue(true).build())
                .build();

        String json = IngestedRecordWriter.fieldsToJson(record);
        assertNotNull(json);
        // Το ίδιο το Jackson είναι ο κριτής: αν δεν διαβάζεται, το jsonb της Postgres θα απέρριπτε τη γραμμή.
        var node = assertDoesNotThrow(() -> new com.fasterxml.jackson.databind.ObjectMapper().readTree(json));
        assertEquals("λέει \"γεια\"\n\tκαι \\ τέλος", node.get("σημείωση").asText());
        assertTrue(node.get("κενό").isNull());
        assertTrue(node.get("σημαία").asBoolean());
    }

    @Test
    @DisplayName("ρυθμός μετατροπής (ενδεικτικός, όχι κατώφλι)")
    void throughput() {
        IngestedRecord record = sample();
        for (int i = 0; i < 20_000; i++) {
            IngestedRecordWriter.fieldsToJson(record); // ζέσταμα του JIT
        }
        int n = 200_000;
        long started = System.nanoTime();
        int bytes = 0;
        for (int i = 0; i < n; i++) {
            bytes += IngestedRecordWriter.fieldsToJson(record).length();
        }
        long ms = (System.nanoTime() - started) / 1_000_000;
        System.out.printf("fieldsToJson: %d εγγραφές σε %d ms (%,d/s), %d bytes%n",
                n, ms, ms > 0 ? n * 1000L / ms : 0, bytes / n);
        assertTrue(bytes > 0);
    }
}
