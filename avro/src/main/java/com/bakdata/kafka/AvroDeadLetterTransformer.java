package com.bakdata.kafka;

import lombok.NonNull;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

public final class AvroDeadLetterTransformer<V> extends DeadLetterTransformer<V, DeadLetter> {

    private AvroDeadLetterTransformer(final @NonNull String description) {
        super(description, new AvroDeadLetterConverter());
    }

    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetter> create(final String description) {
        return () -> new AvroDeadLetterTransformer<>(description);
    }

}
