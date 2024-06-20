description = "A library for error handling in Kafka Streams."

plugins {
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    val avroVersion: String by project
    implementation(group = "org.apache.avro", name = "avro", version = avroVersion)
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")
    implementation(group = "org.apache.commons", name = "commons-lang3", version = "3.14.0")

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    val kafkaStreamsTestsVersion: String by project
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = kafkaStreamsTestsVersion
    )
    testFixturesImplementation(
        testImplementation(
            group = "org.junit.jupiter",
            name = "junit-jupiter-api",
            version = junitVersion
        )
    )
    testFixturesImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = kafkaStreamsTestsVersion
    )
    testFixturesImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    val jacksonVersion: String by project
    testFixturesImplementation(group = "com.fasterxml.jackson.core", name = "jackson-core", version = jacksonVersion)
    testFixturesImplementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = jacksonVersion)
    testFixturesImplementation(group = "com.fasterxml.jackson.datatype", name = "jackson-datatype-jsr310", version = jacksonVersion)
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
