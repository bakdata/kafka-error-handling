import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

description = "Transform dead letters in Kafka Streams applications to protobuf."

plugins {
    idea
    id("com.google.protobuf") version "0.8.18"
}

dependencies {
    api(project(":error-handling-core"))
    api(group = "com.google.protobuf", name = "protobuf-java", version = "3.18.1")
    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(testFixtures(project(":error-handling-core")))
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = "2.4.2"
    )
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-streams-protobuf-serde", version = confluentVersion)
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.18.1"
    }
}
