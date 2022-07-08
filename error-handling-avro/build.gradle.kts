description = "Transform dead letters in Kafka Streams applications to Avro format."

plugins {
    `java-library`
    id("com.github.davidmc24.gradle.plugin.avro") version "1.2.1"
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
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")
    val mockitoVersion: String by project
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    val kafkaStreamsTestsVersion: String by project
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = kafkaStreamsTestsVersion
    )
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
