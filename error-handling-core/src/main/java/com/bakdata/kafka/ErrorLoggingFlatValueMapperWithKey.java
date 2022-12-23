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

import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;

/**
 * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value.
 *
 * @param <K> type of input keys
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #logErrors(ValueMapperWithKey)
 * @see #logErrors(ValueMapperWithKey, Predicate)
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorLoggingFlatValueMapperWithKey<K, V, VR> implements ValueMapperWithKey<K, V, Iterable<VR>> {
    private final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends Iterable<VR>> wrapped;
    private final @NonNull Predicate<Exception> errorFilter;

    /**
     * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value. Recoverable Kafka
     * exceptions such as a schema registry timeout are forwarded and not captured.
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     * @see #logErrors(ValueMapperWithKey, Predicate)
     * @see ErrorUtil#isRecoverable(Exception)
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends Iterable<VR>> mapper) {
        return logErrors(mapper, ErrorUtil::isRecoverable);
    }

    /**
     * Wrap a {@code ValueMapperWithKey} and log thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapperWithKey<K, V, Iterable<VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<K, VR> output = input.flatMapValues(logErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapperWithKey} whose exceptions should be logged
     * @param errorFilter expression that filters errors which should be thrown and not logged
     * @param <K> type of input keys
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapperWithKey}
     */
    public static <K, V, VR> ValueMapperWithKey<K, V, Iterable<VR>> logErrors(
            final @NonNull ValueMapperWithKey<? super K, ? super V, ? extends Iterable<VR>> mapper,
            final @NonNull Predicate<Exception> errorFilter) {
        return new ErrorLoggingFlatValueMapperWithKey<>(mapper, errorFilter);
    }

    @Override
    public Iterable<VR> apply(final K key, final V value) {
        try {
            return this.wrapped.apply(key, value);
        } catch (final Exception e) {
            if (this.errorFilter.test(e)) {
                throw e;
            }
            log.error("Cannot process ('{}', '{}')", ErrorUtil.toString(key), ErrorUtil.toString(value), e);
            return emptyList();
        }
    }
}

