/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_PROCESSOR_NODE_ID_NAME;
import static com.bakdata.kafka.DescribingProcessingExceptionHandler.HEADER_ERRORS_TASK_ID_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@ExtendWith(SoftAssertionsExtension.class)
class DescribingProcessingExceptionHandlerTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    @Mock
    ValueMapper<String, Long> mapper;
    @InjectSoftAssertions
    private SoftAssertions softly;

    private static String getDeserialized(final Headers headers, final String name) {
        try (final Deserializer<String> deserializer = new StringDeserializer()) {
            return deserializer.deserialize(null, headers.lastHeader(name).value());
        }
    }

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC);
        final KStream<Integer, Long> mapped =
                input.mapValues(this.mapper, Named.as("map"));
        mapped.to(OUTPUT_TOPIC, Produced.valueSerde(LONG_SERDE));
    }

    @Override
    protected Map<String, Object> getKafkaProperties() {
        final Map<String, Object> kafkaProperties = super.getKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaProperties.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, ERROR_TOPIC);
        kafkaProperties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                DescribingProcessingExceptionHandler.class);
        return kafkaProperties;
    }

    @Test
    void shouldForwardRecoverableException() {
        final RuntimeException throwable = createRecoverableException();
        when(this.mapper.apply("foo")).thenThrow(throwable);
        this.createTopology();
        final TestInput<Integer, String> input = this.topology.input();
        this.softly.assertThatThrownBy(() -> input.add(1, "foo"))
                .hasCause(throwable);
        final List<ProducerRecord<Integer, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withValueSerde(LONG_SERDE)
                .toList();
        this.softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, String>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .toList();
        this.softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldNotCaptureThrowable() {
        final Throwable throwable = mock(Error.class);
        when(this.mapper.apply("foo")).thenThrow(throwable);
        this.createTopology();
        final TestInput<Integer, String> input = this.topology.input();
        this.softly.assertThatThrownBy(() -> input.add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureValueMapperError() {
        doThrow(new RuntimeException("Cannot process")).when(this.mapper).apply("foo");
        doReturn(2L).when(this.mapper).apply("bar");
        this.createTopology();
        this.topology.input()
                .add(1, "foo")
                .add(2, "bar");
        final List<ProducerRecord<Integer, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withValueSerde(LONG_SERDE)
                .toList();
        this.softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> this.softly.assertThat(producerRecord.key()).isEqualTo(2))
                .extracting(ProducerRecord::value)
                .isInstanceOf(Long.class)
                .satisfies(value -> this.softly.assertThat(value).isEqualTo(2L));
        final List<ProducerRecord<Integer, String>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .toList();
        this.softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isEqualTo(1);
                    this.softly.assertThat(producerRecord.value()).isEqualTo("foo");
                })
                .extracting(ProducerRecord::headers)
                .satisfies(headers -> {
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_EXCEPTION_MESSAGE_NAME))
                            .isEqualTo("Cannot process");
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_STACKTRACE_NAME)).isNotNull();
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_EXCEPTION_NAME))
                            .isEqualTo("java.lang.RuntimeException");
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_TOPIC_NAME))
                            .isEqualTo(INPUT_TOPIC);
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_PARTITION_NAME)).isEqualTo("0");
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_OFFSET_NAME)).isEqualTo("0");
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_PROCESSOR_NODE_ID_NAME))
                            .isEqualTo("map");
                    this.softly.assertThat(getDeserialized(headers, HEADER_ERRORS_TASK_ID_NAME)).isEqualTo("0_0");
                });
    }

    @Test
    void shouldHandleNullInput() {
        when(this.mapper.apply(null)).thenReturn(2L);
        this.createTopology();
        this.topology.input()
                .add(2, null);
        final List<ProducerRecord<Integer, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withValueSerde(LONG_SERDE)
                .toList();
        this.softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> this.softly.assertThat(producerRecord.key()).isEqualTo(2))
                .extracting(ProducerRecord::value)
                .isInstanceOf(Long.class)
                .satisfies(value -> this.softly.assertThat(value).isEqualTo(2L));
        final List<ProducerRecord<Integer, String>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .toList();
        this.softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleErrorOnNullInput() {
        when(this.mapper.apply(null)).thenThrow(new RuntimeException("Cannot process"));
        this.createTopology();
        this.topology.input()
                .add(null, null);
        final List<ProducerRecord<Integer, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withValueSerde(LONG_SERDE)
                .toList();
        this.softly.assertThat(records)
                .isEmpty();
        final List<ProducerRecord<Integer, String>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .toList();
        this.softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> {
                    this.softly.assertThat(producerRecord.key()).isNull();
                    this.softly.assertThat(producerRecord.value()).isNull();
                });
    }

    @Test
    void shouldHandleNullValue() {
        when(this.mapper.apply("bar")).thenReturn(null);
        this.createTopology();
        this.topology.input()
                .add(2, "bar");
        final List<ProducerRecord<Integer, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withValueSerde(LONG_SERDE)
                .toList();
        this.softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> this.softly.assertThat(producerRecord.key()).isEqualTo(2))
                .extracting(ProducerRecord::value)
                .satisfies(value -> this.softly.assertThat(value).isNull());
        final List<ProducerRecord<Integer, String>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .toList();
        this.softly.assertThat(errors)
                .isEmpty();
    }

}
