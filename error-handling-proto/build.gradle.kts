description = "Transform dead letters in Kafka Streams applications to protobuf."

plugins {
    id("com.google.protobuf") version "0.9.1"
}

val protobufVersion: String by project
dependencies {
    api(project(":error-handling-core"))
    api(group = "com.google.protobuf", name = "protobuf-java", version = protobufVersion)
    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(testFixtures(project(":error-handling-core")))
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    val kafkaStreamsTestsVersion: String by project
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = kafkaStreamsTestsVersion
    )
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "schema-registry-mock-junit5",
        version = kafkaStreamsTestsVersion
    )
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}
