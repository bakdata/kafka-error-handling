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

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Wrap a {@code ValueMapper} and describe thrown exceptions with input key and value.
 *
 * @param <V> type of input values
 * @param <VR> type of output values
 * @see #describeErrors(ValueMapper)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorDescribingValueMapper<V, VR> implements ValueMapper<V, VR> {
    private final @NonNull ValueMapper<? super V, ? extends VR> wrapped;

    /**
     * Wrap a {@code ValueMapper} and describe thrown exceptions with input key and value.
     * <pre>{@code
     * final ValueMapper<V, VR> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, VR> output = input.mapValues(describeErrors(mapper));
     * }
     * </pre>
     *
     * @param mapper {@code ValueMapper} whose exceptions should be described
     * @param <V> type of input values
     * @param <VR> type of output values
     * @return {@code ValueMapper}
     */
    public static <V, VR> ValueMapper<V, VR> describeErrors(
            final @NonNull ValueMapper<? super V, ? extends VR> mapper) {
        return new ErrorDescribingValueMapper<>(mapper);
    }

    @Override
    public VR apply(final V value) {
        try {
            return this.wrapped.apply(value);
        } catch (final Exception e) {
            throw new ProcessingException(value, e);
        }
    }
}
