description = "A library for error handling in Kafka Streams."

plugins {
    id("java-library")
    alias(libs.plugins.avro)
}

dependencies {
    api(platform(libs.kafka.bom))
    api(libs.kafka.streams)
    implementation(libs.jool)
    implementation(libs.commons.lang)

    testRuntimeOnly(libs.junit.platform.launcher)
    testFixturesImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(libs.log4j.slf4j2)
    testFixturesApi(libs.fluentKafkaStreamsTests)
    testFixturesImplementation(libs.jackson.core)
    testFixturesImplementation(libs.jackson.databind)
    testFixturesImplementation(libs.jackson.datatype.jsr310)
    testImplementation(libs.avro)
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
