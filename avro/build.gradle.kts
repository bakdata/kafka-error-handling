description = "Serializing errors in Avro."

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
    implementation(group = "org.apache.avro", name = "avro", version = avroVersion)
    implementation(project(":core"))

    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(testFixtures(project(":core")))
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")
    val mockitoVersion = "3.12.4"
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    val log4jVersion = "2.15.0"
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    testImplementation(
        group = "com.bakdata.fluent-kafka-streams-tests",
        name = "fluent-kafka-streams-tests-junit5",
        version = "2.4.2"
    )
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
