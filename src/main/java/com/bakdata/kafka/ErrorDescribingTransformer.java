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
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code Transformer} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of transformation result
 * @see #describeErrors(Transformer)
 */
public final class ErrorDescribingTransformer<K, V, R> extends DecoratorTransformer<K, V, R> {

    private ErrorDescribingTransformer(final @NonNull Transformer<K, V, R> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code Transformer} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(() -> describeErrors(new Transformer<K, V, KeyValue<KR, VR>>() {...}));
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code Transformer}
     */
    public static <K, V, R> Transformer<K, V, R> describeErrors(final @NonNull Transformer<K, V, R> transformer) {
        return new ErrorDescribingTransformer<>(transformer);
    }

    /**
     * Wrap a {@code TransformerSupplier} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final TransformerSupplier<K, V, KeyValue<KR, VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(describeErrors(transformer));
     * }
     * </pre>
     *
     * @param supplier {@code TransformerSupplier} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code TransformerSupplier}
     */
    public static <K, V, R> TransformerSupplier<K, V, R> describeErrors(
            final @NonNull TransformerSupplier<K, V, R> supplier) {
        return new TransformerSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Transformer<K, V, R> get() {
                return describeErrors(supplier.get());
            }
        };
    }

    @Override
    public R transform(final K key, final V value) {
        try {
            return super.transform(key, value);
        } catch (final Exception e) {
            throw new ProcessingException(key, value, e);
        }
    }

}
