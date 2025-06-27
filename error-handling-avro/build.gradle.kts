description = "Transform dead letters in Kafka Streams applications to Avro format."

plugins {
    id("java-library")
    alias(libs.plugins.avro)
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
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.streams)
    api(libs.avro)
    api(project(":error-handling-core"))

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(testFixtures(project(":error-handling-core")))
    testImplementation(libs.mockito.junit)
    testImplementation(libs.assertj)
    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.kafka.streams.avro.serde)
}

avro {
    setGettersReturnOptional(true)
    setOptionalGettersForNullableFieldsOnly(true)
    setFieldVisibility("PRIVATE")
}
