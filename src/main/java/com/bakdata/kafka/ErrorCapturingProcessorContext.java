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

import lombok.NonNull;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;

final class ErrorCapturingProcessorContext extends DecoratorProcessorContext {
    ErrorCapturingProcessorContext(final @NonNull ProcessorContext wrapped) {
        super(wrapped);
    }

    private static <K, V, VR> ProcessedKeyValue<K, V, VR> getValue(final VR value) {
        return SuccessKeyValue.of(value);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey, to);
    }

    @Override
    public <K, V> void forward(final K key, final V value) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey, childIndex);
    }

    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey, childName);
    }
}
