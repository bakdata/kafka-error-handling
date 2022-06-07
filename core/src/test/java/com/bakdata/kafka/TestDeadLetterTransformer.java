package com.bakdata.kafka;

import lombok.NonNull;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

public final class TestDeadLetterTransformer<V> extends DeadLetterTransformer<V, DeadLetterDescription> {

    private TestDeadLetterTransformer(final @NonNull String description) {
        // The 'TestDeadLetter' is just the DeadLetterDescription
        super(description, deadLetterDescription -> deadLetterDescription);
    }

    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetterDescription> create(final String description) {
        return () -> new TestDeadLetterTransformer<>(description);
    }
}
