description = "Transform dead letters in Kafka Streams applications to Avro format."

plugins {
    id("java-library")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

// add .avsc files to jar allowing us to use them in other projects as a schema dependency
sourceSets {
    main {
        resources {
            srcDirs("src/main/avro")
        }
    }
}

dependencies {
    val avroVersion: String by project
    api(group = "org.apache.avro", name = "avro", version = avroVersion)
    api(project(":error-handling-core"))

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
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde")
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
