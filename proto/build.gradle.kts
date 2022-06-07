description = "A library for error handling in Kafka Streams."

plugins {
    idea
    `java-library`
    id("io.freefair.lombok") version "5.3.3.3"
    id("com.google.protobuf") version "0.8.18"
}

group = "com.bakdata.kafka"

tasks.withType<Test> {
    maxParallelForks = 4
    useJUnitPlatform()
}

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    implementation(project(":core"))
    implementation(group = "com.google.protobuf", name = "protobuf-java", version = "3.18.1")
    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    val log4jVersion = "2.15.0"
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
}
