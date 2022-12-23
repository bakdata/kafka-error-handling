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
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code FixedKeyProcessor} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #captureErrors(FixedKeyProcessor)
 * @see #captureErrors(FixedKeyProcessor, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingValueProcessor<K, V, VR>
        implements FixedKeyProcessor<K, V, ProcessedValue<V, VR>> {
    private final @NonNull FixedKeyProcessor<K, V, VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;
    private FixedKeyProcessorContext<K, ProcessedValue<V, VR>> context;

    /**
     * Wrap a {@code FixedKeyProcessor} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @param processor {@code FixedKeyProcessor} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessor}
     * @see #captureErrors(FixedKeyProcessor, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> FixedKeyProcessor<K, V, ProcessedValue<V, VR>> captureErrors(
            final @NonNull FixedKeyProcessor<K, V, VR> processor) {
        return captureErrors(processor, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code FixedKeyProcessor} and capture thrown exceptions.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.processValues(() -> captureErrors(new FixedKeyProcessor<K, V, VR>() {...}));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param processor {@code FixedKeyProcessor} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessor}
     */
    public static <K, V, VR> FixedKeyProcessor<K, V, ProcessedValue<V, VR>> captureErrors(
            final @NonNull FixedKeyProcessor<K, V, VR> processor,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorCapturingValueProcessor<>(processor, errorFilter);
    }

    /**
     * Wrap a {@code FixedKeyProcessorSupplier} and capture thrown exceptions. Recoverable Kafka exceptions such as a
     * schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code FixedKeyProcessorSupplier} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessorSupplier}
     * @see #captureErrors(FixedKeyProcessorSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, V, ProcessedValue<V, VR>> captureErrors(
            final @NonNull FixedKeyProcessorSupplier<K, V, VR> supplier) {
        return captureErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code FixedKeyProcessorSupplier} and capture thrown exceptions.
     * <pre>{@code
     * final FixedKeyProcessorSupplier<K, V, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, ProcessedValue<V, VR>> processed = input.processValues(() -> captureErrors(processor.get()));
     * final KStream<K, VR> output = processed.flatMapValues(ProcessedValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMapValues(ProcessedValue::getErrors);
     * }
     * </pre>
     *
     * @param supplier {@code FixedKeyProcessorSupplier} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessorSupplier}
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, V, ProcessedValue<V, VR>> captureErrors(
            final @NonNull FixedKeyProcessorSupplier<K, V, VR> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new FixedKeyProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public FixedKeyProcessor<K, V, ProcessedValue<V, VR>> get() {
                return captureErrors(supplier.get(), errorFilter);
            }
        };
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, ProcessedValue<V, VR>> context) {
        this.wrapped.init(new ErrorCapturingFixedKeyProcessorContext<>(context));
        this.context = context;
    }

    @Override
    public void process(final FixedKeyRecord<K, V> record) {
        try {
            this.wrapped.process(record);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            this.context.forward(record.withValue(ErrorValue.of(record.value(), e)));
        }
    }

}
