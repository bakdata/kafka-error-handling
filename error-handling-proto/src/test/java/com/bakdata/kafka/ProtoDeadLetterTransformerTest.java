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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import com.bakdata.kafka.proto.v1.DeadLetter;
import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.jooq.lambda.Seq;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(SoftAssertionsExtension.class)
class ProtoDeadLetterTransformerTest extends ErrorCaptureTopologyTest {
    @InjectSoftAssertions
    private SoftAssertions softly;
    private static final String ERROR_TOPIC = "errors";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final Serde<DeadLetter> DEAD_LETTER_SERDE = new KafkaProtobufSerde<>(DeadLetter.class);
    public static final String DEAD_LETTER_DESCRIPTION = "Description";
    public static final String ERROR_MESSAGE = "ERROR!";
    @Mock
    KeyValueMapper<Integer, String, KeyValue<Integer, String>> mapper;

    @Override
    protected void buildTopology(final StreamsBuilder builder) {
        final KStream<Integer, String> input = builder.stream(INPUT_TOPIC, Consumed.with(null, STRING_SERDE));
        final KStream<Integer, ProcessedKeyValue<Integer, String, String>> mapped =
                input.map(ErrorCapturingKeyValueMapper.captureErrors(this.mapper));
        mapped.flatMapValues(ProcessedKeyValue::getValues)
                .to(OUTPUT_TOPIC, Produced.valueSerde(STRING_SERDE));
        mapped.flatMap(ProcessedKeyValue::getErrors)
                .transformValues(ProtoDeadLetterConverter.asTransformer(DEAD_LETTER_DESCRIPTION))
                .to(ERROR_TOPIC);
    }

    @Override
    protected void createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        this.buildTopology(builder);
        final Topology topology = builder.build();
        final Properties kafkaProperties = getKafkaProperties();
        kafkaProperties.put(
                 StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaProtobufSerde.class
        );
        final SchemaRegistryMock schemaRegistryMock = new SchemaRegistryMock(List.of(new ProtobufSchemaProvider()));
        this.topology = new TestTopology<Integer, String>(topology, kafkaProperties)
                .withSchemaRegistryMock(schemaRegistryMock);
        this.topology.start();
        DEAD_LETTER_SERDE.configure(
                 Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                this.topology.getSchemaRegistryUrl()), false);
    }

    @Test
    void shouldConvertAndSerializeProtoDeadLetter() {
        when(this.mapper.apply(any(), any())).thenThrow(new RuntimeException(ERROR_MESSAGE));
        this.createTopology();
        this.topology.input(INPUT_TOPIC).withValueSerde(STRING_SERDE)
                .add(1, "foo")
                .add(2, "bar");

        final List<ProducerRecord<Integer, String>> records = Seq.seq(this.topology.streamOutput(OUTPUT_TOPIC)
                        .withValueSerde(STRING_SERDE))
                .toList();
        this.softly.assertThat(records)
                .isEmpty();

        final List<ProducerRecord<Integer, DeadLetter>> errors = Seq.seq(this.topology.streamOutput(ERROR_TOPIC)
                        .withValueSerde(DEAD_LETTER_SERDE)
                        .withValueType(DeadLetter.class))
                .toList();

        this.softly.assertThat(errors)
                .hasSize(2)
                .extracting(ProducerRecord::value).allSatisfy(
                        deadLetter -> {
                            this.softly.assertThat(deadLetter.getDescription()).isEqualTo(DEAD_LETTER_DESCRIPTION);
                            this.softly.assertThat(deadLetter.getCause().getMessage()).extracting(StringValue::getValue)
                                    .isEqualTo(ERROR_MESSAGE);
                            this.softly.assertThat(deadLetter.getCause().getErrorClass()).extracting(StringValue::getValue)
                                    .isEqualTo(RuntimeException.class.getCanonicalName());
                            // We don't check the exact stack trace, but only that it consists of multiple lines
                            this.softly.assertThat(deadLetter.getCause().getStackTrace()).extracting(StringValue::getValue)
                                    .extracting(s -> Arrays.asList(s.split("\n"))).asList().hasSizeGreaterThan(1);
                            this.softly.assertThat(deadLetter.getTopic()).extracting(StringValue::getValue)
                                    .isEqualTo(INPUT_TOPIC);
                            this.softly.assertThat(deadLetter.getPartition()).extracting(Int32Value::getValue).isEqualTo(0);
                        }
                );
        this.softly.assertThat(errors).extracting(ProducerRecord::value).element(0).satisfies(
                deadLetter -> {
                    this.softly.assertThat(deadLetter.getInputValue()).extracting(StringValue::getValue)
                            .isEqualTo("foo");
                    this.softly.assertThat(deadLetter.getOffset()).extracting(Int64Value::getValue).isEqualTo(0L);
                }
        );
        this.softly.assertThat(errors).map(ProducerRecord::value).element(1).satisfies(
                deadLetter -> {
                    this.softly.assertThat(deadLetter.getInputValue()).extracting(StringValue::getValue)
                            .isEqualTo("bar");
                    this.softly.assertThat(deadLetter.getOffset()).extracting(Int64Value::getValue).isEqualTo(1L);
                }
        );

    }
}
