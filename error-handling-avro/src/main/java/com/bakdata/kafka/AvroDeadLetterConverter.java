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

import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;

/**
 * Convert a {@code DeadLetterDescription} to an Avro {@code DeadLetter}
 */
public final class AvroDeadLetterConverter implements DeadLetterConverter<DeadLetter> {

    @Override
    public DeadLetter convert(final DeadLetterDescription deadLetterDescription) {
        return DeadLetter.newBuilder()
                .setInputValue(deadLetterDescription.getInputValue())
                .setCause(ErrorDescription.newBuilder()
                        .setMessage(deadLetterDescription.getCause().getMessage())
                        .setStackTrace(deadLetterDescription.getCause().getStackTrace())
                        .setErrorClass(deadLetterDescription.getCause().getErrorClass())
                        .build())
                .setDescription(deadLetterDescription.getDescription())
                .setTopic(deadLetterDescription.getTopic())
                .setPartition(deadLetterDescription.getPartition())
                .setOffset(deadLetterDescription.getOffset())
                .build();
    }

    /**
     * Creates a transformer that uses the AvroDeadLetterConverter
     *
     * <pre>{@code
     * // Example, this works for all error capturing topologies
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * final KStream<K, DeadLetter> deadLetters = errors.transformValues(
     *                      AvroDeadLetterConverter.asTransformer("Description"));
     * deadLetters.to(ERROR_TOPIC);
     * }
     * </pre>
     *
     * @param description shared description for all errors
     * @param <V> type of the input value
     * @return a transformer supplier
     * @deprecated Use {@link #asProcessor(String)}
     */
    @Deprecated(since = "1.4.0")
    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetter> asTransformer(
            final String description) {
        return DeadLetterTransformer.create(description, new AvroDeadLetterConverter());
    }

    /**
     * Creates a processor that uses the AvroDeadLetterConverter
     *
     * <pre>{@code
     * // Example, this works for all error capturing topologies
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * final KStream<K, DeadLetter> deadLetters = errors.processValues(
     *                      AvroDeadLetterConverter.asProcessor("Description"));
     * deadLetters.to(ERROR_TOPIC);
     * }
     * </pre>
     *
     * @param description shared description for all errors
     * @param <K> type of the input key
     * @param <V> type of the input value
     * @return a processor supplier
     */
    public static <K, V> FixedKeyProcessorSupplier<K, ProcessingError<V>, DeadLetter> asProcessor(
            final String description) {
        return DeadLetterProcessor.create(description, new AvroDeadLetterConverter());
    }

}
