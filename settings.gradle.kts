pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "error-handling"
include("error-handling-core")
include("error-handling-avro")
include("error-handling-proto")
include("error-handling-bom")
