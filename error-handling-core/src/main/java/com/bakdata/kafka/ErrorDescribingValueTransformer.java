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
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code ValueTransformer} and describe thrown exceptions with input key and value.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #describeErrors(ValueTransformer)
 */
public final class ErrorDescribingValueTransformer<V, VR> extends DecoratorValueTransformer<V, VR> {

    private ErrorDescribingValueTransformer(final @NonNull ValueTransformer<V, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code ValueTransformer} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> describeErrors(new ValueTransformer<V, VR>() {...}));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> ValueTransformer<V, VR> describeErrors(final @NonNull ValueTransformer<V, VR> transformer) {
        return new ErrorDescribingValueTransformer<>(transformer);
    }

    /**
     * Wrap a {@code ValueTransformerSupplier} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerSupplier<V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(describeErrors(transformer));
     * }
     * </pre>
     *
     * @param supplier {@code ValueTransformerSupplier} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerSupplier}
     */
    public static <V, VR> ValueTransformerSupplier<V, VR> describeErrors(
            final @NonNull ValueTransformerSupplier<V, VR> supplier) {
        return new ValueTransformerSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public ValueTransformer<V, VR> get() {
                return describeErrors(supplier.get());
            }
        };
    }

    @Override
    public VR transform(final V value) {
        try {
            return super.transform(value);
        } catch (final Exception e) {
            throw new ProcessingException(value, e);
        }
    }

}
