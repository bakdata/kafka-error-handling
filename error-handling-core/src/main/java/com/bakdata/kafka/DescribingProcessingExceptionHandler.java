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

import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.buildDeadLetterQueueRecord;

import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

/**
 * {@link ProcessingExceptionHandler} that sends records causing a processing exception to the dead letter queue. If an
 * exception is deemed recoverable by {@link DescribingProcessingExceptionHandlerConfig#FILTER_CONFIG} it will be
 * forwarded. In addition to the headers provided by
 * {@link org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils}, this handler also adds
 * {@link #HEADER_ERRORS_PROCESSOR_NODE_ID_NAME} and {@link #HEADER_ERRORS_TASK_ID_NAME}.
 */
@NoArgsConstructor
public class DescribingProcessingExceptionHandler implements ProcessingExceptionHandler {
    public static final String HEADER_ERRORS_PROCESSOR_NODE_ID_NAME = "__streams.errors.processor.node.id";
    public static final String HEADER_ERRORS_TASK_ID_NAME = "__streams.errors.task.id";
    private String deadLetterQueueTopic;
    private ErrorFilter filter;

    @Override
    public void configure(final Map<String, ?> configs) {
        if (configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG) != null) {
            this.deadLetterQueueTopic =
                    String.valueOf(configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
        }
        final DescribingProcessingExceptionHandlerConfig config =
                new DescribingProcessingExceptionHandlerConfig(configs);
        this.filter = config.getErrorFilter();
    }

    @Override
    public Response handleError(final ErrorHandlerContext context, final Record<?, ?> record,
            final Exception exception) {
        if (this.filter.isRecoverable(context, record, exception)) {
            return Response.fail();
        }
        final ProducerRecord<byte[], byte[]> dlqRecord =
                buildDeadLetterQueueRecord(this.deadLetterQueueTopic, context.sourceRawKey(), context.sourceRawValue(),
                        context, exception);
        try (final Serializer<String> serializer = new StringSerializer()) {
            dlqRecord.headers()
                    .add(HEADER_ERRORS_PROCESSOR_NODE_ID_NAME, serializer.serialize(null, context.processorNodeId()));
            dlqRecord.headers()
                    .add(HEADER_ERRORS_TASK_ID_NAME, serializer.serialize(null, context.taskId().toString()));
        }
        return Response.resume(List.of(dlqRecord));
    }

}
