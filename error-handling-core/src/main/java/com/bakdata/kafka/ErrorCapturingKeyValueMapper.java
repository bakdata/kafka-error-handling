/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;

/**
 * Wrap a {@code KeyValueMapper} and capture thrown exceptions.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <KR> type of output keys
 * @param <VR> type of output values
 * @see #captureErrors(KeyValueMapper)
 * @see #captureErrors(KeyValueMapper, Predicate)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorCapturingKeyValueMapper<K, V, KR, VR>
        implements KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> {
    private final @NonNull KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code KeyValueMapper} and capture thrown exceptions. Recoverable Kafka exceptions such as a schema
     * registry timeout are forwarded and not captured.
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code KeyValueMapper}
     * @see #captureErrors
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, KR, VR> KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final @NonNull KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper) {
        return captureErrors(mapper, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code KeyValueMapper} and capture thrown exceptions.
     * <pre>{@code
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMap(ProcessedKeyValue::getErrors);
     * }
     * </pre>
     *
     * @param mapper {@code KeyValueMapper} whose exceptions should be captured
     * @param errorFilter expression that filters errors which should be thrown and not captured
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <KR> type of output keys
     * @param <VR> type of output values
     * @return {@code KeyValueMapper}
     */
    public static <K, V, KR, VR> KeyValueMapper<K, V, KeyValue<KR, ProcessedKeyValue<K, V, VR>>> captureErrors(
            final @NonNull KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorCapturingKeyValueMapper<>(mapper, errorFilter);
    }

    @Override
    public KeyValue<KR, ProcessedKeyValue<K, V, VR>> apply(final K key, final V value) {
        try {
            final KeyValue<? extends KR, ? extends VR> newKeyValue = this.wrapped.apply(key, value);
            final ProcessedKeyValue<K, V, VR> recordWithOldKey = SuccessKeyValue.of(newKeyValue.value);
            return KeyValue.pair(newKeyValue.key, recordWithOldKey);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            final ProcessedKeyValue<K, V, VR> errorWithOldKey = ErrorKeyValue.of(key, value, e);
            // new key is only relevant if no error occurs
            return KeyValue.pair(null, errorWithOldKey);
        }
    }
}
