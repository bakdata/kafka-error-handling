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

import java.util.Set;
import lombok.NonNull;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code FixedKeyProcessor} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #describeErrors(FixedKeyProcessor)
 */
public final class ErrorDescribingValueProcessor<K, V, VR> extends DecoratorValueProcessor<K, V, VR> {

    private ErrorDescribingValueProcessor(final @NonNull FixedKeyProcessor<K, V, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code FixedKeyProcessor} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.processValues(() -> describeErrors(new FixedKeyProcessor<K, V, VR>() {...}));
     * }
     * </pre>
     *
     * @param processor {@code FixedKeyProcessor} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessor}
     */
    public static <K, V, VR> FixedKeyProcessor<K, V, VR> describeErrors(
            final @NonNull FixedKeyProcessor<K, V, VR> processor) {
        return new ErrorDescribingValueProcessor<>(processor);
    }

    /**
     * Wrap a {@code FixedKeyProcessorSupplier} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final FixedKeyProcessorSupplier<K, V, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.processValues(describeErrors(processor));
     * }
     * </pre>
     *
     * @param supplier {@code FixedKeyProcessorSupplier} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessorSupplier}
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, V, VR> describeErrors(
            final @NonNull FixedKeyProcessorSupplier<K, V, VR> supplier) {
        return new FixedKeyProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public FixedKeyProcessor<K, V, VR> get() {
                return describeErrors(supplier.get());
            }
        };
    }

    @Override
    public void process(final FixedKeyRecord<K, V> inputRecord) {
        try {
            super.process(inputRecord);
        } catch (final Exception e) {
            throw new ProcessingException(inputRecord.key(), inputRecord.value(), e);
        }
    }

}
