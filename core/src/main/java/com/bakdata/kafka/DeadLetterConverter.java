package com.bakdata.kafka;

@FunctionalInterface
public interface DeadLetterConverter<T> {

    T convert(final DeadLetterDescription deadLetterDescription);
}
