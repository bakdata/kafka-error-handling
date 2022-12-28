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

import static org.jooq.lambda.Seq.seq;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code ValueTransformer} and capture thrown exceptions.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(ValueTransformer)
 * @see #captureErrors(ValueTransformer, Predicate)
 * @deprecated Use {@link ErrorCapturingValueProcessor}
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Deprecated(since = "1.4.0")
public final class ErrorCapturingFlatValueTransformer<V, VR>
        implements ValueTransformer<V, Iterable<ProcessedValue<V, VR>>> {
    private final @NonNull ValueTransformer<? super V, ? extends Iterable<VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueTransformer} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     * @see #captureErrors(ValueTransformer, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <V, VR> ValueTransformer<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final @NonNull ValueTransformer<? super V, ? extends Iterable<VR>> transformer) {
        return captureErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformer} and capture thrown exceptions.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.flatTransformValues(() -> captureErrors(new ValueTransformer<V, Iterable<VR>>() {...}));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code ValueTransformer} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformer}
     */
    public static <V, VR> ValueTransformer<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final @NonNull ValueTransformer<? super V, ? extends Iterable<VR>> transformer,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorCapturingFlatValueTransformer<>(transformer, errorFilter);
    }

    /**
     * Wrap a {@code ValueTransformerSupplier} and capture thrown exceptions. Recoverable Kafka exceptions such as a
     * schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code ValueTransformerSupplier} whose exceptions should be captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerSupplier}
     * @see #captureErrors(ValueTransformerSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <V, VR> ValueTransformerSupplier<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final @NonNull ValueTransformerSupplier<? super V, ? extends Iterable<VR>> supplier) {
        return captureErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueTransformerSupplier} and capture thrown exceptions.
     * <pre>{@code
     * final ValueTransformerSupplier<V, Iterable<VR>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.flatTransformValues(captureErrors(transformer));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param supplier {@code ValueTransformerSupplier} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueTransformerSupplier}
     */
    public static <V, VR> ValueTransformerSupplier<V, Iterable<ProcessedValue<V, VR>>> captureErrors(
            final @NonNull ValueTransformerSupplier<? super V, ? extends Iterable<VR>> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ValueTransformerSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public ValueTransformer<V, Iterable<ProcessedValue<V, VR>>> get() {
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
    public Iterable<ProcessedValue<V, VR>> transform(final V value) {
        try {
            final Iterable<VR> newValues = this.wrapped.transform(value);
            return seq(newValues).map(SuccessValue::of);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            return List.of(ErrorValue.of(value, e));
        }
    }

}
