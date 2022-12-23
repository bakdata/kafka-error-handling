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
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code FixedKeyProcessor} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(FixedKeyProcessor)
 * @see #logErrors(FixedKeyProcessor, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingValueProcessor<K, V, VR> implements FixedKeyProcessor<K, V, VR> {
    private final @NonNull FixedKeyProcessor<K, V, VR> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code FixedKeyProcessor} and log thrown exceptions with input key and value. Recoverable Kafka exceptions
     * such as a schema registry timeout are forwarded and not captured.
     *
     * @param processor {@code FixedKeyProcessor} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessor}
     * @see #logErrors(FixedKeyProcessor, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> FixedKeyProcessor<K, V, VR> logErrors(
            final @NonNull FixedKeyProcessor<K, V, VR> processor) {
        return logErrors(processor, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code FixedKeyProcessor} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.processValues(() -> logErrors(new FixedKeyProcessor<K, V, VR>() {...}));
     * }
     * </pre>
     *
     * @param processor {@code FixedKeyProcessor} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessor}
     */
    public static <K, V, VR> FixedKeyProcessor<K, V, VR> logErrors(
            final @NonNull FixedKeyProcessor<K, V, VR> processor,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorLoggingValueProcessor<>(processor, errorFilter);
    }

    /**
     * Wrap a {@code FixedKeyProcessorSupplier} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @param supplier {@code FixedKeyProcessorSupplier} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessorSupplier}
     * @see #logErrors(FixedKeyProcessorSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, V, VR> logErrors(
            final @NonNull FixedKeyProcessorSupplier<K, V, VR> supplier) {
        return logErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code FixedKeyProcessorSupplier} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final FixedKeyProcessorSupplier<K, V, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.processValues(logErrors(processor));
     * }
     * </pre>
     *
     * @param supplier {@code FixedKeyProcessorSupplier} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code FixedKeyProcessorSupplier}
     */
    public static <K, V, VR> FixedKeyProcessorSupplier<K, V, VR> logErrors(
            final @NonNull FixedKeyProcessorSupplier<K, V, VR> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new FixedKeyProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public FixedKeyProcessor<K, V, VR> get() {
                return logErrors(supplier.get(), errorFilter);
            }
        };
    }

    @Override
    public void close() {
        this.wrapped.close();
    }

    @Override
    public void init(final FixedKeyProcessorContext<K, VR> context) {
        this.wrapped.init(context);
    }

    @Override
    public void process(final FixedKeyRecord<K, V> record) {
        try {
            this.wrapped.process(record);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('{}', '{}')", ErrorUtil.toString(record.key()),
                    ErrorUtil.toString(record.value()), e);
        }
    }

}
