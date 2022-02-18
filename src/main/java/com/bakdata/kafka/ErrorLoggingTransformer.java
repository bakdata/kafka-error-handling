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
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code Transformer} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <R> type of transformation result
 * @see #logErrors(Transformer)
 * @see #logErrors(Transformer, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingTransformer<K, V, R> implements Transformer<K, V, R> {
    private final @NonNull Transformer<? super K, ? super V, ? extends R> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code Transformer} and log thrown exceptions with input key and value. Recoverable Kafka exceptions such
     * as a schema registry timeout are forwarded and not captured.
     *
     * @param transformer {@code Transformer} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code Transformer}
     * @see #logErrors(Transformer, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, R> Transformer<K, V, R> logErrors(
            final @NonNull Transformer<? super K, ? super V, ? extends R> transformer) {
        return logErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code Transformer} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(() -> logErrors(new Transformer<K, V, KeyValue<KR, VR>>() {...}));
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code Transformer}
     */
    public static <K, V, R> Transformer<K, V, R> logErrors(
            final @NonNull Transformer<? super K, ? super V, ? extends R> transformer,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorLoggingTransformer<>(transformer, errorFilter);
    }

    /**
     * Wrap a {@code TransformerSupplier} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code TransformerSupplier} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code TransformerSupplier}
     * @see #logErrors(TransformerSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, R> TransformerSupplier<K, V, R> logErrors(
            final @NonNull TransformerSupplier<? super K, ? super V, ? extends R> supplier) {
        return logErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code TransformerSupplier} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final TransformerSupplier<K, V, KeyValue<KR, VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.transform(logErrors(transformer));
     * }
     * </pre>
     *
     * @param supplier {@code TransformerSupplier} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <R> type of transformation result
     * @return {@code TransformerSupplier}
     */
    public static <K, V, R> TransformerSupplier<K, V, R> logErrors(
            final @NonNull TransformerSupplier<? super K, ? super V, ? extends R> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new TransformerSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Transformer<K, V, R> get() {
                return logErrors(supplier.get(), errorFilter);
            }
        };
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.wrapped.init(context);
    }

    @Override
    public R transform(final K key, final V value) {
        try {
            return this.wrapped.transform(key, value);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('" + ErrorUtil.toString(key) + "', '" + ErrorUtil.toString(value) + "')", e);
            return null;
        }
    }

}
