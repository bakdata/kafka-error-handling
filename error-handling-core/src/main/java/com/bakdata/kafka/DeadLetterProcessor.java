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

import java.time.Instant;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * {@link FixedKeyProcessor} that creates a {@code DeadLetter} from a processing error.
 *
 * @param <K> type of key
 * @param <V> type of value
 * @param <T> the DeadLetter type
 */
@Getter
@RequiredArgsConstructor
public class DeadLetterProcessor<K, V, T> implements FixedKeyProcessor<K, ProcessingError<V>, T> {
    private final @NonNull String description;
    private final @NonNull DeadLetterConverter<T> deadLetterConverter;
    private FixedKeyProcessorContext<K, T> context;

    /**
     * Transforms captured errors for serialization
     *
     * <pre>{@code
     * // Example, this works for all error capturing topologies
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * final DeadLetterConverter<T> deadLetterConverter = ...
     * final KStream<K, T> deadLetters = errors.processValues(
     *                      DeadLetterProcessor.create("Description", deadLetterConverter));
     * deadLetters.to(ERROR_TOPIC);
     * }
     * </pre>
     *
     * @param description shared description for all errors
     * @param deadLetterConverter converter from DeadLetterDescriptions to VR
     * @param <K> type of the input key
     * @param <V> type of the input value
     * @param <VR> type of the output value
     * @return a processor supplier
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, ProcessingError<V>, VR> create(final String description,
            final DeadLetterConverter<VR> deadLetterConverter) {
        return () -> new DeadLetterProcessor<>(description, deadLetterConverter);
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, T> context) {
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<K, ProcessingError<V>> inputRecord) {
        final ProcessingError<V> error = inputRecord.value();
        final Throwable throwable = error.getThrowable();
        final Optional<RecordMetadata> metadata = this.context.recordMetadata();
        final DeadLetterDescription deadLetterDescription = DeadLetterDescription.builder()
                .inputValue(Optional.ofNullable(error.getValue()).map(ErrorUtil::toString).orElse(null))
                .cause(DeadLetterDescription.Cause.builder()
                        .message(throwable.getMessage())
                        .stackTrace(ExceptionUtils.getStackTrace(throwable))
                        .errorClass(throwable.getClass().getName())
                        .build())
                .description(this.description)
                .topic(metadata.map(RecordMetadata::topic).orElse(null))
                .partition(metadata.map(RecordMetadata::partition).orElse(null))
                .offset(metadata.map(RecordMetadata::offset).orElse(null))
                .inputTimestamp(Instant.ofEpochMilli(inputRecord.timestamp()))
                .build();

        final FixedKeyRecord<K, T> outputRecord = inputRecord
                .withValue(this.deadLetterConverter.convert(deadLetterDescription))
                .withTimestamp(this.context.currentSystemTimeMs());

        this.context.forward(outputRecord);
    }

    @Override
    public void close() {
        // do nothing
    }
}
