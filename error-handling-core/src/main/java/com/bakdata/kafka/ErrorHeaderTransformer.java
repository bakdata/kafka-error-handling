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
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * {@link ValueTransformer} that produces a message with the original value and headers detailing the error that has
 * been captured.
 *
 * Headers added by this ValueTransformer:
 * <ul>
 *     <li>{@code __streams.errors.topic}: original input topic of the erroneous record</li>
 *     <li>{@code __streams.errors.partition}: original partition of the input topic of the erroneous record</li>
 *     <li>{@code __streams.errors.offset}: original offset in the partition of the input topic of the erroneous
 *     record</li>
 *     <li>{@code __streams.errors.description}: description of the context in which an exception has been thrown</li>
 *     <li>{@code __streams.errors.exception.class.name}: class of the exception that was captured</li>
 *     <li>{@code __streams.errors.exception.message}: message of the exception that was captured</li>
 *     <li>{@code __streams.errors.exception.stack_trace}: stack trace of the exception that was captured</li>
 * </ul>
 *
 * @param <V> type of value
 * @deprecated Use {@link ErrorHeaderProcessor}
 */
@RequiredArgsConstructor
@Deprecated(since = "1.4.0")
public class ErrorHeaderTransformer<V> implements ValueTransformer<ProcessingError<V>, V> {
    /**
     * Prefix of all headers added by this ValueTransformer
     */
    public static final String HEADER_PREFIX = ErrorHeaderProcessor.HEADER_PREFIX;
    /**
     * Header indicating the original input topic of the erroneous record
     */
    public static final String TOPIC = ErrorHeaderProcessor.TOPIC;
    /**
     * Header indicating the original partition in the input topic of the erroneous record
     */
    public static final String PARTITION = ErrorHeaderProcessor.PARTITION;
    /**
     * Header indicating the original offset in the partition of the input topic of the erroneous record
     */
    public static final String OFFSET = ErrorHeaderProcessor.OFFSET;
    /**
     * Header indicating the description of the context in which an exception has been thrown
     */
    public static final String DESCRIPTION = ErrorHeaderProcessor.DESCRIPTION;
    /**
     * Prefix of all headers detailing the error message added by this ValueTransformer
     */
    public static final String EXCEPTION_PREFIX = ErrorHeaderProcessor.EXCEPTION_PREFIX;
    /**
     * Header indicating the class of the exception that was captured
     */
    public static final String EXCEPTION_CLASS_NAME = ErrorHeaderProcessor.EXCEPTION_CLASS_NAME;
    /**
     * Header indicating the message of the exception that was captured
     */
    public static final String EXCEPTION_MESSAGE = ErrorHeaderProcessor.EXCEPTION_MESSAGE;
    /**
     * Header indicating the stack trace of the exception that was captured
     */
    public static final String EXCEPTION_STACK_TRACE = ErrorHeaderProcessor.EXCEPTION_STACK_TRACE;
    private final @NonNull String description;
    private ProcessorContext context = null;

    /**
     * Create a new {@code ErrorHeaderTransformer} with the provided description
     *
     * @param description description of the context in which an exception has been thrown
     * @param <V> type of value
     * @return {@code ValueTransformerSupplier} that produces a message with the original value and headers detailing
     * the error that has been captured.
     */
    public static <V> ValueTransformerSupplier<ProcessingError<V>, V> withErrorHeaders(final String description) {
        return () -> new ErrorHeaderTransformer<>(description);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public V transform(final ProcessingError<V> value) {
        this.addHeader(TOPIC, this.context.topic());
        this.addHeader(PARTITION, Integer.toString(this.context.partition()));
        this.addHeader(OFFSET, Long.toString(this.context.offset()));
        this.addHeader(EXCEPTION_CLASS_NAME, value.getThrowable().getClass().getName());
        this.addHeader(EXCEPTION_MESSAGE, value.getThrowable().getMessage());
        this.addHeader(EXCEPTION_STACK_TRACE, ExceptionUtils.getStackTrace(value.getThrowable()));
        this.addHeader(DESCRIPTION, this.description);
        return value.getValue();
    }

    @Override
    public void close() {
        // do nothing
    }

    private void addHeader(final String key, final String value) {
        final Headers headers = this.context.headers();
        ErrorHeaderProcessor.addHeader(key, value, headers);
    }
}
