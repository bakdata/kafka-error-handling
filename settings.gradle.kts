pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

rootProject.name = "error-handling"
include("error-handling-core")
include("error-handling-avro")
include("error-handling-proto")
