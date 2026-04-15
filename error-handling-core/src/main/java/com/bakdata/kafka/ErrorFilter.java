package com.bakdata.kafka;

import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

@FunctionalInterface
public interface ErrorFilter {
    boolean isRecoverable(ErrorHandlerContext context, Record<?, ?> record, Exception exception);
}
