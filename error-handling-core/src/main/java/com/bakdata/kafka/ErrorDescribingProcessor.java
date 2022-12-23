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
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;

/**
 * Wrap a {@code Processor} and describe thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #describeErrors(Processor)
 */
public final class ErrorDescribingProcessor<K, V, KR, VR> extends DecoratorProcessor<K, V, KR, VR> {

    private ErrorDescribingProcessor(final @NonNull Processor<K, V, KR, VR> wrapped) {
        super(wrapped);
    }

    /**
     * Wrap a {@code Processor} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.process(() -> describeErrors(new Processor<K, V, KR, VR>() {...}));
     * }
     * </pre>
     *
     * @param processor {@code Processor} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Processor}
     */
    public static <K, V, KR, VR> Processor<K, V, KR, VR> describeErrors(
            final @NonNull Processor<K, V, KR, VR> processor) {
        return new ErrorDescribingProcessor<>(processor);
    }

    /**
     * Wrap a {@code ProcessorSupplier} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ProcessorSupplier<K, V, KR, VR> processor = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.process(describeErrors(processor));
     * }
     * </pre>
     *
     * @param supplier {@code ProcessorSupplier} whose exceptions should be described
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code ProcessorSupplier}
     */
    public static <K, V, KR, VR> ProcessorSupplier<K, V, KR, VR> describeErrors(
            final @NonNull ProcessorSupplier<K, V, KR, VR> supplier) {
        return new ProcessorSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Processor<K, V, KR, VR> get() {
                return describeErrors(supplier.get());
            }
        };
    }

    @Override
    public void process(final Record<K, V> record) {
        try {
            super.process(record);
        } catch (final Exception e) {
            throw new ProcessingException(record.key(), record.value(), e);
        }
    }

}
