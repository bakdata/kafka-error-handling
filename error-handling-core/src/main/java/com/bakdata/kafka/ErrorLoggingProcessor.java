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

import java.util.Set;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code Processor} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #logErrors(Processor)
 * @see #logErrors(Processor, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingProcessor<K, V, KR, VR> implements Processor<K, V, KR, VR> {
    private final @NonNull Processor<K, V, KR, VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code Processor} and log thrown exceptions with input key and value. Recoverable Kafka exceptions such as
     * a schema registry timeout are forwarded and not captured.
     *
     * @param processor {@code Processor} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Processor}
     * @see #logErrors(Processor, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> Processor<K, V, KR, VR> logErrors(
            final @NonNull Processor<K, V, KR, VR> processor) {
        return logErrors(processor, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code Processor} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.process(() -> logErrors(new Processor<K, V, KR, VR>() {...}));
     * }
     * </pre>
     *
     * @param processor {@code Processor} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Processor}
     */
    public static <K, V, KR, VR> Processor<K, V, KR, VR> logErrors(
            final @NonNull Processor<K, V, KR, VR> processor,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorLoggingProcessor<>(processor, errorFilter);
    }

    /**
     * Wrap a {@code ProcessorSupplier} and log thrown exceptions with input key and value. Recoverable Kafka exceptions
     * such as a schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code ProcessorSupplier} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code ProcessorSupplier}
     * @see #logErrors(ProcessorSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> ProcessorSupplier<K, V, KR, VR> logErrors(
            final @NonNull ProcessorSupplier<K, V, KR, VR> supplier) {
        return logErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ProcessorSupplier} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ProcessorSupplier<K, V, KR, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.process(logErrors(processor));
     * }
     * </pre>
     *
     * @param supplier {@code ProcessorSupplier} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code ProcessorSupplier}
     */
    public static <K, V, KR, VR> ProcessorSupplier<K, V, KR, VR> logErrors(
            final @NonNull ProcessorSupplier<K, V, KR, VR> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Processor<K, V, KR, VR> get() {
                return logErrors(supplier.get(), errorFilter);
            }
        };
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final ProcessorContext<KR, VR> context) {
        this.wrapped.init(context);
    }

    @Override
    public void process(final Record<K, V> inputRecord) {
        try {
            this.wrapped.process(inputRecord);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('{}', '{}')", ErrorUtil.toString(inputRecord.key()),
                    ErrorUtil.toString(inputRecord.value()), e);
        }
    }

}
