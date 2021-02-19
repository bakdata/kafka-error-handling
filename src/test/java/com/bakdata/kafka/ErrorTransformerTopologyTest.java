/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(SoftAssertionsExtension.class)
class ErrorTransformerTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    @Mock
    KeyValueMapper<Integer, String, KeyValue<Double, Long>> mapper;

    private static String getHeader(final Headers headers, final String key) {
        return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
    }

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mapped =
                input.map(ErrorCapturingKeyValueMapper.captureErrors(this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .transformValues(() -> new ErrorTransformer<>("Description"))
                .to(ERROR_TOPIC, Produced.with(null, STRING_SERDE));
    }

    @Test
    void shouldCaptureKeyValueMapperError(final SoftAssertions softly) {
        doReturn(KeyValue.pair(1.0, 1L)).when(this.mapper).apply(1, "foo");
        doThrow(new RuntimeException("Cannot process")).when(this.mapper).apply(2, "bar");
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(1.0))
                .extracting(ProducerRecord::value)
                .isInstanceOf(Long.class)
                .satisfies(value -> softly.assertThat(value).isEqualTo(1L));
        final List<ProducerRecord<Integer, String>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueSerde(Serdes.String()))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> {
                    softly.assertThat(record.key()).isEqualTo(2);
                    softly.assertThat(record.value()).isEqualTo("bar");
                })
                .extracting(ProducerRecord::headers)
                .satisfies(headers -> {
                    softly.assertThat(getHeader(headers, ErrorTransformer.OFFSET)).isEqualTo("1");
                    softly.assertThat(getHeader(headers, ErrorTransformer.PARTITION)).isEqualTo("0");
                    softly.assertThat(getHeader(headers, ErrorTransformer.TOPIC)).isEqualTo(INPUT_TOPIC);
                    softly.assertThat(getHeader(headers, ErrorTransformer.DESCRIPTION)).isEqualTo("Description");
                    softly.assertThat(getHeader(headers, ErrorTransformer.EXCEPTION_MESSAGE))
                            .isEqualTo("Cannot process");
                    softly.assertThat(getHeader(headers, ErrorTransformer.EXCEPTION_CLASS_NAME))
                            .isEqualTo(RuntimeException.class.getName());
                    softly.assertThat(getHeader(headers, ErrorTransformer.EXCEPTION_STACK_TRACE)).isNotNull();
                });
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null, null)).thenThrow(new RuntimeException("Cannot process"));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();
        final List<ProducerRecord<Integer, String>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                .withValueSerde(Serdes.String()))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> {
                    softly.assertThat(record.key()).isNull();
                    softly.assertThat(record.value()).isNull();
                });
    }

}
