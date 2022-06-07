package com.bakdata.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TestDeadLetterSerde implements Serde<DeadLetterDescription> {
    static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Serializer<DeadLetterDescription> serializer = (topic, data) -> {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (final JsonProcessingException e) {
            throw new SerializationException(e);
        }
    };
    private static final Deserializer<DeadLetterDescription> deserializer = (topic, data) -> {
        try {
            final JsonNode json = objectMapper.readTree(data);
            final JsonNode causeJson = json.get("cause");
            return DeadLetterDescription.builder()
                    .inputValue(asTextOrNull(json.get("inputValue")))
                    .cause(DeadLetterDescription.Cause.builder()
                            .message(asTextOrNull(causeJson.get("message")))
                            .errorClass(causeJson.get("errorClass").asText())
                            .stackTrace(causeJson.get("stackTrace").asText())
                            .build())
                    .description(asTextOrNull(json.get("description")))
                    .topic(json.get("topic").asText())
                    .partition(json.get("partition").asInt())
                    .offset(json.get("offset").asLong())
                    .build();
        } catch (IOException e) {
            throw new SerializationException(e);
        }
    };

    private static String asTextOrNull(final JsonNode json) {
        return json == null || json.isNull() ? null : json.asText();
    }

    @Override
    public Serializer<DeadLetterDescription> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<DeadLetterDescription> deserializer() {
        return deserializer;
    }
}
