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

import com.bakdata.kafka.proto.v1.ProtoDeadLetter;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;


/**
 * Convert a {@code DeadLetterDescription} to a {@code ProtoDeadLetter} message
 */
public class ProtoDeadLetterConverter implements DeadLetterConverter<ProtoDeadLetter> {

    @Override
    public ProtoDeadLetter convert(final DeadLetterDescription deadLetterDescription) {
        final ProtoDeadLetter.Builder builder = ProtoDeadLetter.newBuilder();
        final DeadLetterDescription.Cause cause = deadLetterDescription.getCause();
        final ProtoDeadLetter.Cause.Builder causeBuilder = builder.getCauseBuilder();
        // Everything is optional with fix defaults in proto3, so use wrappers
        if (cause.getMessage() != null) {
            causeBuilder.setMessage(StringValue.of(cause.getMessage()));
        }
        if (cause.getStackTrace() != null) {
            causeBuilder.setStackTrace(StringValue.of(cause.getStackTrace()));
        }
        if (cause.getErrorClass() != null) {
            causeBuilder.setErrorClass(StringValue.of(cause.getErrorClass()));
        }
        builder.setDescription(deadLetterDescription.getDescription());
        if (deadLetterDescription.getInputValue() != null) {
            builder.setInputValue(StringValue.of(deadLetterDescription.getInputValue()));
        }
        if (deadLetterDescription.getTopic() != null) {
            builder.setTopic(StringValue.of(deadLetterDescription.getTopic()));
        }
        if (deadLetterDescription.getPartition() != null) {
            builder.setPartition(Int32Value.of(deadLetterDescription.getPartition()));
        }
        if (deadLetterDescription.getOffset() != null) {
            builder.setOffset(Int64Value.of(deadLetterDescription.getOffset()));
        }
        return builder.build();
    }

    /**
     * Creates a transformer that uses the ProtoDeadLetterConverter
     *
     * <pre>{@code
     * // Example, this works for all error capturing topologies
     * final KeyValueMapper<K, V, KeyValue<KR, VR>> mapper = ...;
     * final KStream<K, V> input = ...;
     * final KStream<KR, ProcessedKeyValue<K, V, VR>> processed = input.map(captureErrors(mapper));
     * final KStream<KR, VR> output = processed.flatMapValues(ProcessedKeyValue::getValues);
     * final KStream<K, ProcessingError<V>> errors = processed.flatMapValues(ProcessedKeyValue::getErrors);
     * final KStream<K, ProtoDeadLetter> deadLetters = errors.transformValues(
     *                      ProtoDeadLetterConverter.asTransformer("Description"));
     * deadLetters.to(OUTPUT_TOPIC);
     * }
     * </pre>
     *
     * @param description shared description for all errors
     * @param <V> type of the input value
     * @return a transformer supplier
     */
    public static <V> ValueTransformerSupplier<ProcessingError<V>, ProtoDeadLetter> asTransformer(final String description) {
        return DeadLetterTransformer.create(description, new ProtoDeadLetterConverter());
    }
}
