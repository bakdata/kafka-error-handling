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
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

final class ErrorCapturingApiProcessorContext<K, V, KR, VR> extends DecoratorProcessingContext
        implements ProcessorContext<KR, VR> {
    private final @NonNull ProcessorContext<? super KR, ? super ProcessedKeyValue<K, V, VR>> wrapped;

    ErrorCapturingApiProcessorContext(
            final @NonNull ProcessorContext<? super KR, ? super ProcessedKeyValue<K, V, VR>> wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    private static <K, KR, KForward extends KR, V, VR, VForward extends VR> Record<KForward, ProcessedKeyValue<K, V,
            VR>> getValue(final Record<KForward, VForward> record) {
        final VR value = record.value();
        final ProcessedKeyValue<K, V, VR> recordWithOldKey = SuccessKeyValue.of(value);
        return record.withValue(recordWithOldKey);
    }

    @Override
    public <KForward extends KR, VForward extends VR> void forward(final Record<KForward, VForward> record) {
        final Record<KForward, ProcessedKeyValue<K, V, VR>> recordWithOldKey = getValue(record);
        this.wrapped.forward(recordWithOldKey);
    }

    @Override
    public <KForward extends KR, VForward extends VR> void forward(final Record<KForward, VForward> record,
            final String childName) {
        final Record<KForward, ProcessedKeyValue<K, V, VR>> recordWithOldKey = getValue(record);
        this.wrapped.forward(recordWithOldKey, childName);
    }
}
