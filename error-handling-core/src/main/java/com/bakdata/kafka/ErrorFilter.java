/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Classify processing errors as recoverable and thus transition the Kafka Streams instance into a error state.
 */
@FunctionalInterface
public interface ErrorFilter {
    /**
     * Determine if a processing error is recoverable
     *
     * @param context Processing context metadata.
     * @param record Record where the exception occurred.
     * @param exception The actual exception.
     * @return if the processing error is recoverable
     */
    boolean isRecoverable(ErrorHandlerContext context, Record<?, ?> record, Exception exception);
}
