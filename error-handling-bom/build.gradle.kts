description = "BOM for error handling in Kafka Streams."

plugins {
    id("java-platform")
}

dependencies {
    constraints {
        api(project(":error-handling-core"))
        api(project(":error-handling-avro"))
        api(project(":error-handling-proto"))
    }
}
