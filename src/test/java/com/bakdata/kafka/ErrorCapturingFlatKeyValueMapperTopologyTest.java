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

import static com.bakdata.kafka.FilterHelper.filterAll;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
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
class ErrorCapturingFlatKeyValueMapperTopologyTest extends ErrorCaptureTopologyTest {
    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    @Mock
    KeyValueMapper<Integer, String, Iterable<KeyValue<Double, Long>>> mapper;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));

        final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mapped =
                input.flatMap(ErrorCapturingFlatKeyValueMapper.captureErrors(this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .transformValues(DeadLetterTransformer.createDeadLetter("Description"))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldNotAllowNullMapper(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingFlatKeyValueMapper.captureErrors(null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingFlatKeyValueMapper.captureErrors(null, filterAll()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotAllowNullFilter(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingFlatKeyValueMapper.captureErrors(this.mapper, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardSchemaRegistryTimeout(final SoftAssertions softly) {
        when(this.mapper.apply(1, "foo")).thenThrow(createSchemaRegistryTimeoutException());
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .hasCauseInstanceOf(SerializationException.class);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withKeySerde(DOUBLE_SERDE)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldNotCaptureThrowable(final SoftAssertions softly) {
        final Throwable throwable = mock(Error.class);
        when(this.mapper.apply(1, "foo")).thenThrow(throwable);
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureKeyValueMapperError(final SoftAssertions softly) {
        doThrow(new RuntimeException("Cannot process")).when(this.mapper).apply(1, "foo");
        doReturn(List.of(KeyValue.pair(2.0, 2L), KeyValue.pair(1.0, 1L), KeyValue.pair(18.0, 18L))).when(this.mapper)
                .apply(2, "bar");
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
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder(2.0, 1.0, 18.0);
        softly.assertThat(records)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(2L, 1L, 18L);

        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(1))
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetter.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).hasValue("foo");
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    final ErrorDescription cause = deadLetter.getCause();
                    softly.assertThat(cause.getMessage()).hasValue("Cannot process");
                    softly.assertThat(cause.getStackTrace()).isPresent();
                    softly.assertThat(cause.getErrorClass()).hasValue("java.lang.RuntimeException");
                    softly.assertThat(deadLetter.getTopic()).hasValue(INPUT_TOPIC);
                    softly.assertThat(deadLetter.getPartition()).hasValue(0);
                    softly.assertThat(deadLetter.getOffset()).hasValue(0L);
                });
    }

    @Test
    void shouldHandleNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null, null)).thenReturn(List.of(KeyValue.pair(2.0, 2L), KeyValue.pair(3.0, 3L)));

        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withKeySerde(DOUBLE_SERDE)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .extracting(ProducerRecord::key)
                .containsExactlyInAnyOrder(2.0, 3.0);
        softly.assertThat(records)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(2L, 3l);
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
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
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isNull())
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetter.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).isNotPresent();
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    final ErrorDescription cause = deadLetter.getCause();
                    softly.assertThat(cause.getMessage()).hasValue("Cannot process");
                    softly.assertThat(cause.getStackTrace()).isPresent();
                    softly.assertThat(cause.getErrorClass()).hasValue("java.lang.RuntimeException");
                    softly.assertThat(deadLetter.getTopic()).hasValue(INPUT_TOPIC);
                    softly.assertThat(deadLetter.getPartition()).hasValue(0);
                    softly.assertThat(deadLetter.getOffset()).hasValue(0L);
                });
    }

    @Test
    void shouldHandleNullKeyValue(final SoftAssertions softly) {
        when(this.mapper.apply(2, "bar")).thenReturn(List.of(KeyValue.pair(null, null)));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar");
        final List<ProducerRecord<Double, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withKeySerde(DOUBLE_SERDE)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isNull())
                .extracting(ProducerRecord::value)
                .satisfies(value -> softly.assertThat(value).isNull());
        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetter.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

}