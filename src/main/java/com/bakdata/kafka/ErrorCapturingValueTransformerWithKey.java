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
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueTransformerWithKey)
 * @see #captureErrors(ValueTransformerWithKey, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingValueTransformerWithKey<K, V, VR>
        implements ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> {
    private final @NonNull ValueTransformerWithKey<? super K, ? super V, ? extends VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions. Recoverable Kafka exceptions such as a
     * schema registry timeout are forwarded and not captured.
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     * @see #captureErrors(ValueTransformerWithKey, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer) {
        return captureErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerWithKey} and capture thrown exceptions.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.transformValues(() -> captureErrors(new ValueTransformerWithKey<K, V, VR>() {...}));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformerWithKey} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKey}
     */
    public static <K, V, VR> ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKey<? super K, ? super V, ? extends VR> transformer,
            final Predicate<Exception> errorFilter) {
        return new ErrorCapturingValueTransformerWithKey<>(transformer, errorFilter);
    }

    /**
     * Wrap a {@code ValueTransformerWithKeySupplier} and capture thrown exceptions. Recoverable Kafka exceptions such
     * as a schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code ValueTransformerWithKeySupplier} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKeySupplier}
     * @see #captureErrors(ValueTransformerWithKeySupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueTransformerWithKeySupplier<K, V, ProcessedValue<V, VR>> captureErrors(
            final ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> supplier) {
        return captureErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerWithKeySupplier} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerWithKeySupplier<K, V, VR> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.transformValues(() -> captureErrors(transformer.get()));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = input.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param supplier {@code ValueTransformerWithKeySupplier} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerWithKeySupplier}
     */
    public static <K, V, VR> ValueTransformerWithKeySupplier<K, V, ProcessedValue<V, VR>> captureErrors(
            final @NonNull ValueTransformerWithKeySupplier<? super K, ? super V, ? extends VR> supplier,
            final Predicate<Exception> errorFilter) {
        return new ValueTransformerWithKeySupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public ValueTransformerWithKey<K, V, ProcessedValue<V, VR>> get() {
                return captureErrors(supplier.get(), errorFilter);
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
    public ProcessedValue<V, VR> transform(final K key, final V value) {
        try {
            final VR newValue = this.wrapped.transform(key, value);
            return SuccessValue.of(newValue);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return ErrorValue.of(value, e);
        }
    }

}
