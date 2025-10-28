description = "Transform dead letters in Kafka Streams applications to protobuf."

plugins {
    id("java-library")
    alias(libs.plugins.protobuf)
}

val protobufVersion = libs.protobuf.get().version
dependencies {
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.streams)
    api(project(":error-handling-core"))
    api(libs.protobuf)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(testFixtures(project(":error-handling-core")))
    testImplementation(libs.mockito.junit)
    testImplementation(libs.assertj)
    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.kafka.streams.protobuf.serde) {
        exclude(group = "org.apache.kafka") // force usage of OSS kafka-clients
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
}
