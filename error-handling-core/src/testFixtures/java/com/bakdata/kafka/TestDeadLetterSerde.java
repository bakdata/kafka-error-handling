/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

public class TestDeadLetterSerde implements Serde<DeadLetterDescription> {
    static final ObjectMapper objectMapper = JsonMapper.builder().build();
    private static final Serializer<DeadLetterDescription> serializer = (topic, data) -> {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (final JacksonException e) {
            throw new SerializationException(e);
        }
    };
    private static final Deserializer<DeadLetterDescription> deserializer = (topic, data) -> {
        try {
            return objectMapper.readValue(data, DeadLetterDescription.class);
        } catch (JacksonException e) {
            throw new SerializationException(e);
        }
    };

    @Override
    public Serializer<DeadLetterDescription> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<DeadLetterDescription> deserializer() {
        return deserializer;
    }
}
