package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordMapperTest {

    private static final String PAYLOAD = """
            {"btcusdt": {"min": 100000.0, "max": 70000000.0},
             "ethusdt": {"min": 5000.0, "max": 10000.0},
             "items": [{"p": 1}, {"p": 2}], "name": "x"}""";

    @Test
    void mapsPayloadValuesToDynamicProtoFields() throws Exception {
        JsonNode root = new ObjectMapper().readTree(PAYLOAD);
        Map<String, String> m = new LinkedHashMap<>();
        m.put("EthPrice", "$.ethusdt.max");
        m.put("BtcMin", "$.btcusdt.min");
        m.put("SecondItem", "$.items[1].p");
        m.put("Name", "name");           // σκέτο όνομα = κλειδί πρώτου επιπέδου
        m.put("lat", "$.ethusdt.min");   // σταθερό πεδίο
        m.put("Missing", "$.nope.x");    // δεν υπάρχει -> παραλείπεται

        IngestedRecord r = IngestedRecord.parseFrom(
                RecordMapper.map("src", PAYLOAD, root, m).toByteArray());

        assertEquals(10000.0, r.getFieldsOrThrow("EthPrice").getNumberValue());
        assertEquals(100000.0, r.getFieldsOrThrow("BtcMin").getNumberValue());
        assertEquals(2.0, r.getFieldsOrThrow("SecondItem").getNumberValue());
        assertEquals("x", r.getFieldsOrThrow("Name").getStringValue());
        assertEquals(5000.0, r.getLat());
        assertFalse(r.containsFields("Missing"));
        assertEquals(PAYLOAD, r.getPayloadJson());
    }

    @Test
    void messageKeyComesFromKeyField() throws Exception {
        JsonNode root = new ObjectMapper().readTree(PAYLOAD);

        assertEquals("x", RecordMapper.keyOf(root, "name"));
        assertEquals("10000.0", RecordMapper.keyOf(root, "$.ethusdt.max"));
        assertEquals("1", RecordMapper.keyOf(root, "$.items[0].p"));
        // Χωρίς keyField, ή με διαδρομή που δεν βρίσκει τιμή, το μήνυμα φεύγει χωρίς κλειδί
        // (μοιράζεται σε όλα τα partitions).
        assertNull(RecordMapper.keyOf(root, null));
        assertNull(RecordMapper.keyOf(root, "  "));
        assertNull(RecordMapper.keyOf(root, "$.nope.x"));
    }

    @Test
    void nullMappingsAreAccepted() throws Exception {
        JsonNode root = new ObjectMapper().readTree(PAYLOAD);
        assertEquals(0, RecordMapper.map("s", PAYLOAD, root, null).getFieldsCount());
    }
}
