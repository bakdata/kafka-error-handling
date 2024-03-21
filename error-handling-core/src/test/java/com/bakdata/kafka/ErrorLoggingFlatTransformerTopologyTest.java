/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
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
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorLoggingFlatTransformerTopologyTest extends ErrorCaptureTopologyTest {
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    private Transformer<Integer, String, Iterable<KeyValue<Double, Long>>> mapper = null;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));

        final KStream<Double, Long> mapped =
                input.flatTransform(ErrorLoggingFlatTransformer.logErrors(() -> this.mapper));
        mapped.to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
    }

    @Test
    void shouldNotAllowNullMapper(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(
                        (Transformer<? super Object, ? super Object, ? extends Iterable<Object>>) null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(
                        (Transformer<? super Object, ? super Object, ? extends Iterable<Object>>) null, filterAll()))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(
                        (TransformerSupplier<? super Object, ? super Object, ? extends Iterable<Object>>) null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(
                        (TransformerSupplier<? super Object, ? super Object, ? extends Iterable<Object>>) null,
                        filterAll()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotAllowNullFilter(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(this.mapper, null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorLoggingFlatTransformer.logErrors(() -> this.mapper, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardSerializationException(final SoftAssertions softly) {
        final RuntimeException throwable = new SerializationException();
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key.equals(1) && "foo".equals(value)) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
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
    }

    @Test
    void shouldNotCaptureError(final SoftAssertions softly) {
        final Error throwable = mock(Error.class);
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key.equals(1) && "foo".equals(value)) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureKeyValueMapperError(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {
            private ProcessorContext context = null;

            @Override
            public void init(final ProcessorContext context) {
                this.context = context;
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key.equals(1) && "foo".equals(value)) {
                    throw new RuntimeException("Cannot process");
                }
                if (key.equals(2) && "bar".equals(value)) {
                    return List.of(KeyValue.pair(2.0, 2L), KeyValue.pair(1.0, 1L), KeyValue.pair(18.0, 18L));
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
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
    }

    @Test
    void shouldReturnOnNullInput(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key == null && value == null) {
                    return List.of(KeyValue.pair(2.0, 2L), KeyValue.pair(3.0, 3L));
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
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
                .containsExactlyInAnyOrder(2L, 3L);
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key == null && value == null) {
                    throw new RuntimeException("Cannot process");
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
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
    }

    @Test
    void shouldHandleNullKeyValue(final SoftAssertions softly) {
        this.mapper = new Transformer<>() {

            @Override
            public void init(final ProcessorContext context) {
            }

            @Override
            public Iterable<KeyValue<Double, Long>> transform(final Integer key, final String value) {
                if (key.equals(2) && "bar".equals(value)) {
                    return List.of(KeyValue.pair(null, null));
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {

            }
        };
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
    }

}
