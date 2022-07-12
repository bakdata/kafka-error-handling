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

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
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
class ErrorCapturingFlatValueMapperTopologyTest extends ErrorCaptureTopologyTest {
    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    @Mock
    ValueMapper<String, Iterable<Long>> mapper;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));

        final KStream<Integer, ProcessedValue<String, Long>> mapped =
                input.flatMapValues(ErrorCapturingFlatValueMapper.captureErrors(this.mapper));

        mapped.flatMapValues(ProcessedValue::getValues)
                .to(OUTPUT_TOPIC, Produced.valueSerde(LONG_SERDE));

        mapped.flatMapValues(ProcessedValue::getErrors)
                .transformValues(DeadLetterTransformer.create("Description", deadLetterDescription -> deadLetterDescription))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldNotAllowNullMapper(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingFlatValueMapper.captureErrors(null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingFlatValueMapper.captureErrors(null, filterAll()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotAllowNullFilter(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingFlatValueMapper.captureErrors(this.mapper, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardSchemaRegistryTimeout(final SoftAssertions softly) {
        when(this.mapper.apply("foo")).thenThrow(createSchemaRegistryTimeoutException());
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .hasCauseInstanceOf(SerializationException.class);
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldNotCaptureThrowable(final SoftAssertions softly) {
        final Throwable throwable = mock(Error.class);
        when(this.mapper.apply("foo")).thenThrow(throwable);
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureValueMapperError(final SoftAssertions softly) {
        doThrow(new RuntimeException("Cannot process")).when(this.mapper).apply("foo");
        doReturn(List.of(6L, 15L, 15L)).when(this.mapper).apply("bar");
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar");
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(3)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(6L, 15L, 15L);

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isEqualTo(1))
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetterDescription.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).isEqualTo("foo");
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    final DeadLetterDescription.Cause cause = deadLetter.getCause();
                    softly.assertThat(cause.getMessage()).isEqualTo("Cannot process");
                    softly.assertThat(cause.getStackTrace()).isNotNull();
                    softly.assertThat(cause.getErrorClass()).isEqualTo("java.lang.RuntimeException");
                    softly.assertThat(deadLetter.getTopic()).isEqualTo(INPUT_TOPIC);
                    softly.assertThat(deadLetter.getPartition()).isEqualTo(0);
                    softly.assertThat(deadLetter.getOffset()).isEqualTo(0L);
                });
    }

    @Test
    void shouldHandleNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null)).thenReturn(List.of(2L, 5L));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, null);
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(2)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(2L, 5L);

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        when(this.mapper.apply(null)).thenThrow(new RuntimeException("Cannot process"));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .isEmpty();
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class))
                .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(record -> softly.assertThat(record.key()).isNull())
                .extracting(ProducerRecord::value)
                .isInstanceOf(DeadLetterDescription.class)
                .satisfies(deadLetter -> {
                    softly.assertThat(deadLetter.getInputValue()).isNull();
                    softly.assertThat(deadLetter.getDescription()).isEqualTo("Description");
                    final DeadLetterDescription.Cause cause = deadLetter.getCause();
                    softly.assertThat(cause.getMessage()).isEqualTo("Cannot process");
                    softly.assertThat(cause.getStackTrace()).isNotNull();
                    softly.assertThat(cause.getErrorClass()).isEqualTo("java.lang.RuntimeException");
                    softly.assertThat(deadLetter.getTopic()).isEqualTo(INPUT_TOPIC);
                    softly.assertThat(deadLetter.getPartition()).isEqualTo(0);
                    softly.assertThat(deadLetter.getOffset()).isEqualTo(0L);
                });
    }

    @Test
    void shouldHandleNullValue(final SoftAssertions softly) {
        when(this.mapper.apply("bar")).thenReturn(Arrays.asList(5L, null, 2L));
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar");
        final List<ProducerRecord<Integer, Long>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(LONG_SERDE))
                .toList();
        softly.assertThat(records)
                .hasSize(3)
                .extracting(ProducerRecord::value)
                .containsExactlyInAnyOrder(5L, null, 2L);

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class))
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

}
