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

import com.bakdata.fluent_kafka_streams_tests.TestInput;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorCapturingValueProcessorSerializationTopologyTest extends ErrorCaptureTopologyTest {

    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Integer> INTEGER_SERDE = Serdes.Integer();
    private static final Serde<String> THROWING_SERDE = Serdes.serdeFrom(
            (topic, data) -> {
                throw new SerializationException("Cannot serialize");
            },
            (topic, data) -> {
                throw new UnsupportedOperationException();
            }
    );

    private static FixedKeyProcessor<Integer, String, String> createMapper() {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<Integer, String> context = null;

            @Override
            public void init(final FixedKeyProcessorContext<Integer, String> context) {
                this.context = context;
            }

            @Override
            public void process(final FixedKeyRecord<Integer, String> inputRecord) {
                this.context.forward(inputRecord);
            }

            @Override
            public void close() {
                // do nothing
            }
        };
    }

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Integer, ProcessedValue<String, String>> mapped = input.processValues(
                ErrorCapturingValueProcessor.captureErrors(
                        ErrorCapturingValueProcessorSerializationTopologyTest::createMapper));
        mapped.flatMapValues(ProcessedValue::getValues)
                .to(OUTPUT_TOPIC, Produced.with(INTEGER_SERDE, THROWING_SERDE));
        mapped.flatMapValues(ProcessedValue::getErrors)
                .processValues(
                        DeadLetterProcessor.create("Description", deadLetterDescription -> deadLetterDescription))
                .to(ERROR_TOPIC);
    }

    @Test
    void shouldNotCaptureSerializationErrorOfDownstreamSink(final SoftAssertions softly) {
        this.createTopology();
        final TestInput<Integer, String> input = this.topology.input()
                .withValueSerde(STRING_SERDE);
        softly.assertThatThrownBy(() -> input.add(1, "foo"))
                .hasRootCauseInstanceOf(SerializationException.class)
                .hasRootCauseMessage("Cannot serialize");

        final List<ProducerRecord<Integer, DeadLetterDescription>> errors = this.topology.streamOutput(ERROR_TOPIC)
                .withValueType(DeadLetterDescription.class)
                .toList();
        softly.assertThat(errors)
                .isEmpty();
    }
}
