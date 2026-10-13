package com.thesis.kafkacollector.mapping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thesis.kafkacollector.proto.IngestedRecordProto.IngestedRecord;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Ελέγχει serialize/deserialize μέσω (mock) Schema Registry, χωρίς Kafka ή δίκτυο. */
class ProtobufRegistryRoundTripTest {

    @Test
    void recordSurvivesSerializerAndDeserializer() throws Exception {
        String json = "{\"ethusdt\":{\"max\":10000.0}}";
        IngestedRecord record = RecordMapper.map("src", json,
                new ObjectMapper().readTree(json), Map.of("EthPrice", "$.ethusdt.max"));

        try (KafkaProtobufSerializer<IngestedRecord> ser = new KafkaProtobufSerializer<>();
             KafkaProtobufDeserializer<IngestedRecord> de = new KafkaProtobufDeserializer<>()) {
            ser.configure(Map.of("schema.registry.url", "mock://roundtrip"), false);
            de.configure(Map.of("schema.registry.url", "mock://roundtrip",
                    "specific.protobuf.value.type", IngestedRecord.class.getName()), false);

            byte[] bytes = ser.serialize("ais-data", record);
            IngestedRecord back = de.deserialize("ais-data", bytes);

            assertEquals(record, back);
            assertEquals(10000.0, back.getFieldsOrThrow("EthPrice").getNumberValue());
        }
    }
}
