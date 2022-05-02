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

    @Deprecated
    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey, childIndex);
    }

    @Deprecated
    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        final ProcessedKeyValue<Object, Object, V> recordWithOldKey = getValue(value);
        super.forward(key, recordWithOldKey, childName);
    }
}
