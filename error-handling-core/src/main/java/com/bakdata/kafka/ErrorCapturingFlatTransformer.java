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

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.jooq.lambda.Seq;

/**
 * Wrap a {@code KeyValueMapper} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #captureErrors(Transformer)
 * @see #captureErrors(Transformer, Predicate)
 * @deprecated Use {@link ErrorCapturingProcessor}
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Deprecated(since = "1.4.0")
public final class ErrorCapturingFlatTransformer<K, V, KR, VR>
        implements Transformer<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> {
    private final @NonNull Transformer<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code Transformer} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema registry
     * timeout are forwarded and not captured.
     *
     * @param transformer {@code Transformer} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Transformer}
     * @see #captureErrors(Transformer, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> Transformer<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>>
    captureErrors(final @NonNull Transformer<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> transformer) {
        return captureErrors(transformer, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code Transformer} and capture thrown exceptions.
     * <pre>{@code
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.flatTransform(() -> captureErrors(new Transformer<K, V, Iterable<KeyValue<KR, VR>>>() {...}));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param transformer {@code Transformer} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code Transformer}
     */
    public static <K, V, KR, VR> Transformer<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>>
    captureErrors(final @NonNull Transformer<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> transformer,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorCapturingFlatTransformer<>(transformer, errorFilter);
    }

    /**
     * Wrap a {@code TransformerSupplier} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @param supplier {@code TransformerSupplier} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code TransformerSupplier}
     * @see #captureErrors(TransformerSupplier, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> TransformerSupplier<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> captureErrors(
            final @NonNull TransformerSupplier<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> supplier) {
        return captureErrors(supplier, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code TransformerSupplier} and capture thrown exceptions.
     * <pre>{@code
     * final TransformerSupplier<K, V, Iterable<KeyValue<KR, VR>>> transformer = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.flatTransform(captureErrors(transformer));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param supplier {@code TransformerSupplier} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code TransformerSupplier}
     */
    public static <K, V, KR, VR> TransformerSupplier<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> captureErrors(
            final @NonNull TransformerSupplier<? super K, ? super V, ? extends Iterable<KeyValue<KR, VR>>> supplier,
            final @NonNull Predicate<Exception> errorFilter) {
        return new TransformerSupplier<>() {
            @Override
            public Set<StoreBuilder<?>> stores() {
                return supplier.stores();
            }

            @Override
            public Transformer<K, V, Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>>> get() {
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
        this.wrapped.init(new ErrorCapturingProcessorContext(context));
    }

    @Override
    public Iterable<KeyValue<KR, ProcessedKeyValue<K, V, VR>>> transform(final K key, final V value) {
        try {
            final Iterable<KeyValue<KR, VR>> newKeyValues = this.wrapped.transform(key, value);
            if (newKeyValues == null) {
                return null;
            }
            return Seq.seq(newKeyValues)
                    .map(kv -> KeyValue.pair(kv.key, SuccessKeyValue.of(kv.value)));
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(key, value, e);
            // new key is only relevant if no error occurs
            return List.of(KeyValue.pair(null, errorWithOldKey));
        }
    }

}
