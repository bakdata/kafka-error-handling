description = "Transform dead letters in Kafka Streams applications to protobuf."

plugins {
    id("java-library")
    id("com.google.protobuf") version "0.9.4"
}

val protobufVersion: String by project
dependencies {
    api(project(":error-handling-core"))
    api(group = "com.google.protobuf", name = "protobuf-java", version = protobufVersion)
    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(testFixtures(project(":error-handling-core")))
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    val kafkaStreamsTestsVersion: String by project
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = kafkaStreamsTestsVersion
    )
    val kafkaUtilsVersion: String by project
    testImplementation(platform("com.bakdata.kafka:confluent-bom:$kafkaUtilsVersion"))
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}
