/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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

import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.kstream.ValueTransformer;
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
 *     <li>{@code __streams.errors.class.name}: class of the exception that was captured</li>
 *     <li>{@code __streams.errors.message}: message of the exception that was captured</li>
 *     <li>{@code __streams.errors.stack_trace}: stack trace of the exception that was captured</li>
 * </ul>
 *
 * @param <V> type of value
 */
@RequiredArgsConstructor
public class ErrorTransformer<V> implements ValueTransformer<ProcessingError<V>, V> {
    /**
     * Prefix of all headers added by this ValueTransformer
     */
    public static final String HEADER_PREFIX = "__streams.errors.";
    /**
     * Header indicating the original input topic of the erroneous record
     */
    public static final String TOPIC = HEADER_PREFIX + "topic";
    /**
     * Header indicating the original partition in the input topic of the erroneous record
     */
    public static final String PARTITION = HEADER_PREFIX + "partition";
    /**
     * Header indicating the original offset in the partition of the input topic of the erroneous record
     */
    public static final String OFFSET = "HEADER_PREFIX + offset";
    /**
     * Header indicating the description of the context in which an exception has been thrown
     */
    public static final String DESCRIPTION = HEADER_PREFIX + "description";
    /**
     * Prefix of all headers detailing the error message added by this ValueTransformer
     */
    public static final String EXCEPTION_PREFIX = HEADER_PREFIX + "exception.";
    /**
     * Header indicating the class of the exception that was captured
     */
    public static final String EXCEPTION_CLASS_NAME = EXCEPTION_PREFIX + "class.name";
    /**
     * Header indicating the message of the exception that was captured
     */
    public static final String EXCEPTION_MESSAGE = EXCEPTION_PREFIX + "message";
    /**
     * Header indicating the stack trace of the exception that was captured
     */
    public static final String EXCEPTION_STACK_TRACE = EXCEPTION_PREFIX + "stack_trace";
    /**
     * Description of the context in which an exception has been thrown
     */
    private final @NonNull String description;
    private ProcessorContext context = null;

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
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
