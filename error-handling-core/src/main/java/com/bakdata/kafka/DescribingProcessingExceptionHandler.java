package com.bakdata.kafka;

import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.buildDeadLetterQueueRecord;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

public class DescribingProcessingExceptionHandler implements ProcessingExceptionHandler {
    public static final String HEADER_ERRORS_PROCESSOR_NODE_ID_NAME = "__streams.errors.processor.node.id";
    public static final String HEADER_ERRORS_TASK_ID_NAME = "__streams.errors.task.id";
    private String deadLetterQueueTopic = null;
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
