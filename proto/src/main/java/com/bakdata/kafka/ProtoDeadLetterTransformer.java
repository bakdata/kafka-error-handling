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

import com.bakdata.kafka.proto.v1.Deadletter.DeadLetter;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import lombok.NonNull;

/**
 * Transform error representations to a {@code DeadLetter} message
 *
 * @param <V> type of the input value
 */
public final class ProtoDeadLetterTransformer<V> extends DeadLetterTransformer<V, DeadLetter> {

    private ProtoDeadLetterTransformer(final @NonNull String description) {
        super(description, new ProtoDeadLetterConverter());
    }

    /**
     * Transforms the captured errors to protobuf for serialization
     *
     * Example:
     * <pre>
     * // Capture errors in the topology
     * final KStream<...> mapped = streamsBuilder.mapValues(
     *              ErrorCapturingValueMapper.captureErrors(mapper));
     *     ...
     * // Use the transformer to serialize the errors as protobuf
     * mapped.flatMapValues(ProcessedValue::getErrors)
     *     .transformValues(ProtoDeadLetterTransformer.create("Description"))
     *     .to(ERROR_TOPIC, Produced.valueSerde(DEAD_LETTER_SERDE));
     * <pre/>
     *
     * @param description shared description for all errors
     * @param <V> type of the input value
     * @return a transformer supplier
     */
    public static <V> ValueTransformerSupplier<ProcessingError<V>, DeadLetter> create(final String description) {
        return () -> new ProtoDeadLetterTransformer<>(description);
    }
}
