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

import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * {@link ValueTransformer} that creates a {@code DeadLetter} from a processing error.
 *
 * @param <V> type of value
 * @param <T> the DeadLetter type
 */
@Getter
@RequiredArgsConstructor
public class DeadLetterTransformer<V, T> implements ValueTransformer<ProcessingError<V>, T> {
    private final @NonNull String description;
    private final @NonNull DeadLetterConverter<T> deadLetterConverter;
    private ProcessorContext context = null;

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
     * final DeadLetterConverter<V> deadLetterConverter = ...
     * errors.transformValues(DeadLetterTransformer.create("Description", deadLetterConverter))
     *       .to(ERROR_TOPIC);
     * }
     * </pre>
     *
     * @param description shared description for all errors
     * @param <V> type of the input value
     * @return a transformer supplier
     */
    public static <V, VR> ValueTransformerSupplier<ProcessingError<V>, VR> create(final String description, final DeadLetterConverter<VR> deadLetterConverter) {
        return () -> new DeadLetterTransformer<>(description, deadLetterConverter);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public T transform(final ProcessingError<V> error) {
        final DeadLetterDescription deadLetterDescription = DeadLetterDescription.builder()
                .inputValue(Optional.ofNullable(error.getValue()).map(ErrorUtil::toString).orElse(null))
                .cause(DeadLetterDescription.Cause.builder()
                        .message(error.getThrowable().getMessage())
                        .stackTrace(ExceptionUtils.getStackTrace(error.getThrowable()))
                        .errorClass(error.getThrowable().getClass().getName())
                        .build())
                .description(this.description)
                .topic(this.context.topic())
                .partition(this.context.partition())
                .offset(this.context.offset())
                .build();
        return this.deadLetterConverter.convert(deadLetterDescription);
    }

    @Override
    public void close() {
        // do nothing
    }
}
