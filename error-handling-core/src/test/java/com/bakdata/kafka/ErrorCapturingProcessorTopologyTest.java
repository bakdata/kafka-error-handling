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

import static com.bakdata.kafka.FilterHelper.filterAll;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorCapturingProcessorTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static final Serde<Double> DOUBLE_SERDE = Serdes.Double();
    private Processor<Integer, String, Double, Long> mapper = null;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mapped =
                input.process(ErrorCapturingProcessor.captureErrors(() -> this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(DOUBLE_SERDE, LONG_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .processValues(
                        DeadLetterProcessor.create("Description", deadLetterDescription -> deadLetterDescription))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldNotAllowNullProcessor(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(
                        (Processor<? super Object, ? super Object, ?, ?>) null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(
                        (Processor<? super Object, ? super Object, ?, ?>) null, filterAll()))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(
                        (ProcessorSupplier<? super Object, ? super Object, ?, ?>) null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(
                        (ProcessorSupplier<? super Object, ? super Object, ?, ?>) null, filterAll()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldNotAllowNullFilter(final SoftAssertions softly) {
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(this.mapper, null))
                .isInstanceOf(NullPointerException.class);
        softly.assertThatThrownBy(() -> ErrorCapturingProcessor.captureErrors(() -> this.mapper, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardRecoverableException(final SoftAssertions softly) {
        final RuntimeException throwable = createRecoverableException();
        this.mapper = new Processor<>() {

            @Override
            public void init(final ProcessorContext context) {
                // do nothing
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (1 == record.key() && "foo".equals(record.value())) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .hasCause(throwable);
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldNotCaptureThrowable(final SoftAssertions softly) {
        final Error throwable = mock(Error.class);
        this.mapper = new Processor<>() {

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (1 == record.key() && "foo".equals(record.value())) {
                    throw throwable;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        softly.assertThatThrownBy(() -> this.topology.input()
                        .withValueSerde(STRING_SERDE)
                        .add(1, "foo"))
                .isEqualTo(throwable);
    }

    @Test
    void shouldCaptureProcessorError(final SoftAssertions softly) {
        this.mapper = new Processor<>() {
            private ProcessorContext<Double, Long> context = null;

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (1 == record.key() && "foo".equals(record.value())) {
                    throw new RuntimeException("Cannot process");
                }
                if (2 == record.key() && "bar".equals(record.value())) {
                    this.context.forward(record.withKey(2.0).withValue(2L));
                    return;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar");
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                );
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isEqualTo(1))
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
    void shouldReturnOnNullInput(final SoftAssertions softly) {
        this.mapper = new Processor<>() {
            private ProcessorContext<Double, Long> context = null;

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (record.key() == null && record.value() == null) {
                    this.context.forward(record.withKey(2.0).withValue(2L));
                    return;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                );
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleErrorOnNullInput(final SoftAssertions softly) {
        this.mapper = new Processor<>() {

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (record.key() == null && record.value() == null) {
                    throw new RuntimeException("Cannot process");
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .isEmpty();
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .hasSize(1)
                .first()
                .isNotNull()
                .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isNull())
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
    void shouldForwardOnNullInput(final SoftAssertions softly) {
        this.mapper = new Processor<>() {
            private ProcessorContext<Double, Long> context = null;

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (record.key() == null && record.value() == null) {
                    this.context.forward(record.withKey(3.0).withValue(3L));
                    return;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(null, null);
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .hasSize(1)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isEqualTo(3.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(3L))
                );
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

    @Test
    void shouldHandleForwardedNullKeyValue(final SoftAssertions softly) {
        this.mapper = new Processor<>() {
            private ProcessorContext<Double, Long> context = null;

            @Override
            public void init(final ProcessorContext<Double, Long> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<Integer, String> record) {
                if (2 == record.key() && "bar".equals(record.value())) {
                    this.context.forward(record.withKey(2.0).withValue(2L));
                    return;
                }
                if (3 == record.key() && "baz".equals(record.value())) {
                    this.context.forward(record.<Double>withKey(null).withValue(null));
                    return;
                }
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };
        this.createTopology();
        this.topology.input()
                .withValueSerde(STRING_SERDE)
                .add(2, "bar")
                .add(3, "baz");
        final List<ProducerRecord<Double, Long>> records = this.topology.streamOutput(OUTPUT_TOPIC)
                .withKeySerde(DOUBLE_SERDE)
                .withValueSerde(LONG_SERDE)
                .toList();
        softly.assertThat(records)
                .hasSize(2)
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isEqualTo(2.0))
                        .extracting(ProducerRecord::value)
                        .isInstanceOf(Long.class)
                        .satisfies(value -> softly.assertThat(value).isEqualTo(2L))
                )
                .anySatisfy(r -> softly.assertThat(r)
                        .isNotNull()
                        .satisfies(producerRecord -> softly.assertThat(producerRecord.key()).isNull())
                        .extracting(ProducerRecord::value)
                        .satisfies(value -> softly.assertThat(value).isNull())
                );
        final List<ProducerRecord<Integer, DeadLetterDescription>> errors =
                this.topology.streamOutput(ERROR_TOPIC)
                        .withValueType(DeadLetterDescription.class)
                        .toList();
        softly.assertThat(errors)
                .isEmpty();
    }

}
