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
    public static final String HEADER_ERRORS_DESCRIPTION_NAME = "__streams.errors.description";
    private String deadLetterQueueTopic = null;
    private ErrorFilter filter;

    @Override
    public void configure(final Map<String, ?> configs) {
        if (configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG) != null) {
            this.deadLetterQueueTopic =
                    String.valueOf(configs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG));
        }
        final ErrorFilterProcessingExceptionHandlerConfig config =
                new ErrorFilterProcessingExceptionHandlerConfig(configs);
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
            final String description =
                    String.format("processor node: %s, taskId: %s", context.processorNodeId(), context.taskId());
            dlqRecord.headers().add(HEADER_ERRORS_DESCRIPTION_NAME, serializer.serialize(null, description));
        }
        return Response.resume(List.of(dlqRecord));
    }

}
