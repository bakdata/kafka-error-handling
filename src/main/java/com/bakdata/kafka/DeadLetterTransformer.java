/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * {@link ValueTransformer} that creates a {@code DeadLetter} from a processing error.
 *
 * @param <V> type of value
 */
@RequiredArgsConstructor
public class DeadLetterTransformer<V> implements ValueTransformer<ProcessingError<V>, DeadLetter> {
    private final @NonNull String description;
    private ProcessorContext context = null;

    /**
     * Create a new {@code DeadLetterTransformer} with the provided description
     *
     * @param description description of the context in which an exception has been thrown
     * @param <V> type of value
     * @return {@code ValueTransformerSupplier} that produces a dead letter message created from a processing error.
     */
    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetter> createDeadLetter(
            final String description) {
        return () -> new DeadLetterTransformer<>(description);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public DeadLetter transform(final ProcessingError<V> value) {
        return value.newDeadLetterBuilder(this.description)
                .setTopic(this.context.topic())
                .setPartition(this.context.partition())
                .setOffset(this.context.offset())
                .build();
    }

    @Override
    public void close() {
        // do nothing
    }
}
