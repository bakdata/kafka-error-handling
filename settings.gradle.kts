pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

rootProject.name = "error-handling"
include("error-handling-core")
include("error-handling-avro")
include("error-handling-proto")
include("error-handling-bom")
