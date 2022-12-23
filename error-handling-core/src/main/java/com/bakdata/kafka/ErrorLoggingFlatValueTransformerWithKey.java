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

import static java.util.Collections.emptyList;

import java.util.Set;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(ValueTransformerWithKey)
 * @see #logErrors(ValueTransformerWithKey, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingFlatValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     * @see #logErrors(ValueTransformerWithKey, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> transformer) {
        return logErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(() -> logErrors(new ValueTransformerWithKey<K, V, Iterable<VR>>() {...}));
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends Iterable<VR>> transformer,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorLoggingFlatValueTransformerWithKey<>(transformer, errorFilter);
    }

    /**
     * Wrap a {@code ValueTransformerWithKeySupplier} and log thrown exceptions with input key and value. Recoverable
     * Kafka exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code ValueTransformerWithKeySupplier} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKeySupplier}
     * @see #logErrors(ValueTransformerWithKeySupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKeySupplier<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueTransformerWithKeySupplier<? super K, ? super V, ? extends Iterable<VR>> supplier) {
        return logErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerWithKeySupplier} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, Iterable<VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.transformValues(logErrors(transformer));
     * }
     * </pre>
     *
     * @param supplier {@code ValueTransformerWithKeySupplier} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKeySupplier}
     */
    public static <K, V, VR> ValueTransformerWithKeySupplier<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueTransformerWithKeySupplier<? super K, ? super V, ? extends Iterable<VR>> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ValueTransformerWithKeySupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public ValueTransformerWithKey<K, V, Iterable<VR>> get() {
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
    public Iterable<VR> transform(final K key, final V value) {
        try {
            return this.wrapped.transform(key, value);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('{}', '{}')", ErrorUtil.toString(key), ErrorUtil.toString(value), e);
            return emptyList();
        }
    }

}
