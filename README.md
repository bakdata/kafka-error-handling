[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-error-handling?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=23&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Aerror-handling&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Aerror-handling)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Aerror-handling&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Aerror-handling)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/error-handling-core.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:error-handling-core&core=gav)

# Kafka error handling
Libraries for error handling in [Kafka Streams](https://kafka.apache.org/documentation/streams/).

## Getting Started

You can add Kafka error handling via Maven Central.
Depending on how you want to store the dead letters in Kafka, you can use the Avro or Protobuf converter. 

#### Gradle
```gradle
// for Avro dead letters
compile group: 'com.bakdata.kafka', name: 'error-handling-avro', version: '1.3.0'
// or, for Protobuf dead letters
compile group: 'com.bakdata.kafka', name: 'error-handling-proto', version: '1.3.0'
// or, for custom dead letters
compile group: 'com.bakdata.kafka', name: 'error-handling-core', version: '1.3.0'
```

#### Maven
```xml
<!-- for Avro dead letters -->
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>error-handling-avro</artifactId>
    <version>1.3.0</version>
</dependency>

<!-- or, for Protobuf dead letters -->
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>error-handling-proto</artifactId>
    <version>1.3.0</version>
</dependency>

<!-- or, for custom dead letters -->
<dependency>
<groupId>com.bakdata.kafka</groupId>
<artifactId>error-handling-core</artifactId>
<version>1.3.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/error-handling/latest).

### Usage

If you use Kafka Streams to process your data, you will sooner or later get to the point where processing of a message throws an exception.
In case your streams application is configured to process every message at least once, which is the case most times,
your application will crash upon encountering an error and retry processing the erroneous message.
If the error was just temporary, processing will continue as if nothing has happened.
However, if the error is related to the specific message, then your streams application will be stuck processing the record.
For such cases we developed three solutions that help handling errors in your Kafka Streams application.

Consider the following Topology:

```java
final KeyValueMapper<Integer, String, KeyValue<Double, Long>> mapper = …
final KStream<Integer, String> input =
       builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()));

final KStream<Double, Long> mapped = input.map(mapper);
mapped.to(OUTPUT_TOPIC, Produced.with(Serdes.Double(), Serdes.Long()));
```

You can simply add a dead letter queue to your topology with our libraries:

```java
final KeyValueMapper<Integer, String, KeyValue<Double, Long>> mapper = …
final KStream<Integer, String> input =
       builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()));

final KStream<Double, ProcessedKeyValue<Integer, String, Long>> mappedWithErrors =
       input.map(captureErrors(mapper));
mappedWithErrors.flatMap(ProcessedKeyValue::getErrors)
       .processValues(AvroDeadLetterConverter.asProcessor("A good description where the pipeline broke"))
       .to(ERROR_TOPIC);

final KStream<Double, Long> mapped = mappedWithErrors.flatMapValues(ProcessedKeyValue::getValues);
mapped.to(OUTPUT_TOPIC, Produced.with(Serdes.Double(), Serdes.Long()));
```

Successfully processed messages are sent to the output topic as before.
However, errors are sent to a specific error topic.
This error topic contains dead letters describing the input value, error message and stack trace of any error that is raised in that part of your topology.

The example uses the `AvroDeadLetterConverter` from `error-handling-avro`.
Analogously, `error-handling-proto` implements a `ProtoDeadLetterConverter`.
A custom `DeadLetterConverter` can be passed to `DeadLetterProcessor.create`.

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-error-handling.git
> cd kafka-error-handling && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-error-handling/blob/master/LICENSE) for more details.
