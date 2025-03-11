plugins {
    id("com.bakdata.release") version "1.7.1"
    id("com.bakdata.sonar") version "1.7.1"
    id("com.bakdata.sonatype") version "1.8.2-SNAPSHOT"
    id("io.freefair.lombok") version "8.12.2"
}

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
        maven(url = "https://packages.confluent.io/maven/")
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

subprojects {
    publication {
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
            developer {
                name.set("Markus Kobold")
                id.set("mkcode92")
            }
        }
    }
}
