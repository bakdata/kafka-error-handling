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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code Processor} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #captureErrors(Processor)
 * @see #captureErrors(Processor, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingProcessor<K, V, KR, VR>
        implements Processor<K, V, KR, ProcessedKeyValue<K, V, VR>> {
    private final @NonNull Processor<K, V, KR, VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;
    private ProcessorContext<KR, ProcessedKeyValue<K, V, VR>> context;

    /**
     * Wrap a {@code Processor} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema registry
     * timeout are forwarded and not captured.
     *
     * @param processor {@code Processor} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Processor}
     * @see #captureErrors(Processor, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> Processor<K, V, KR, ProcessedKeyValue<K, V, VR>> captureErrors(
            final @NonNull Processor<K, V, KR, VR> processor) {
        return captureErrors(processor, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code Processor} and capture thrown exceptions.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.process(() -> captureErrors(new Processor<K, V, KR, VR>() {...}));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param processor {@code Processor} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Processor}
     */
    public static <K, V, KR, VR> Processor<K, V, KR, ProcessedKeyValue<K, V, VR>> captureErrors(
            final @NonNull Processor<K, V, KR, VR> processor,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorCapturingProcessor<>(processor, errorFilter);
    }

    /**
     * Wrap a {@code ProcessorSupplier} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @param supplier {@code ProcessorSupplier} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code ProcessorSupplier}
     * @see #captureErrors(ProcessorSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> ProcessorSupplier<K, V, KR, ProcessedKeyValue<K, V, VR>> captureErrors(
            final @NonNull ProcessorSupplier<K, V, KR, VR> supplier) {
        return captureErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ProcessorSupplier} and capture thrown exceptions.
     * <pre>{@code
     * final ProcessorSupplier<K, V, KR, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.process(captureErrors(processor));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param supplier {@code ProcessorSupplier} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code ProcessorSupplier}
     */
    public static <K, V, KR, VR> ProcessorSupplier<K, V, KR, ProcessedKeyValue<K, V, VR>> captureErrors(
            final @NonNull ProcessorSupplier<K, V, KR, VR> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Processor<K, V, KR, ProcessedKeyValue<K, V, VR>> get() {
                return captureErrors(supplier.get(), errorFilter);
            }
        };
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext<KR, ProcessedKeyValue<K, V, VR>> context) {
        this.wrapped.init(new ErrorCapturingApiProcessorContext<K, V, KR, VR>(context));
        this.context = context;
    }

    @Override
    public void process(final Record<K, V> record) {
        try {
            this.wrapped.process(record);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(record.key(), record.value(), e);
            // new key is only relevant if no error occurs
            this.context.forward(record.<KR>withKey(null).withValue(errorWithOldKey));
        }
    }

}
