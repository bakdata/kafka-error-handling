description = "A library for error handling in Kafka Streams."

plugins {
    `java-library`
    id("net.researchgate.release") version "2.8.0"
    id("com.bakdata.sonar") version "1.1.4"
    id("com.bakdata.sonatype") version "1.1.4"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "3.8.0"
    id("com.commercehub.gradle.plugin.avro") version "0.16.0"
}

group = "com.bakdata.kafka"

tasks.withType<Test> {
    maxParallelForks = 4
    useJUnitPlatform()
}

repositories {
    mavenCentral()
    maven(url = "http://packages.confluent.io/maven/")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    val avroVersion: String by project
    api(group = "org.apache.avro", name = "avro", version = avroVersion)
    implementation(group = "org.jooq", name = "jool", version = "0.9.14")
    implementation(group = "org.apache.commons", name = "commons-lang3", version = "3.9")

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")
    val mockitoVersion = "2.28.2"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)
    testImplementation(group = "log4j", name = "log4j", version = "1.2.17")
    testImplementation(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.26")
    testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = "2.1.0")
    val confluentVersion: String by project
    testImplementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Sven Lehmann")
            id.set("SvenLehmann")
        }
        developer {
            name.set("Torben Meyer")
            id.set("torbsto")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    githubRepository = "kafka-error-handling"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}
