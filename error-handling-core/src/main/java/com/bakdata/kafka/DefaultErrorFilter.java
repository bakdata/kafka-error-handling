package com.bakdata.kafka;

import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

public class DefaultErrorFilter implements ErrorFilter {
    @Override
    public boolean isRecoverable(final ErrorHandlerContext context, final Record<?, ?> record,
            final Exception exception) {
        return ErrorUtil.isRecoverable(exception);
    }
}
