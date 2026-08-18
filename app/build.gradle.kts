plugins {
    java
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("com.gradleup.shadow") version "8.3.6"
}

group = "dev.kotryos"
version = "1.0.0"

// Kafka 3.6.x / Confluent 7.6.x are a matched pair. The Confluent serializer
// version must equal the cp-schema-registry image tag in docker-compose.yml.
val kafkaVersion = "3.6.1"
val confluentVersion = "7.6.1"
val avroVersion = "1.11.3"
val slf4jVersion = "1.7.36" // Kafka 3.6.x is built against the slf4j 1.7 API

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("io.confluent:kafka-avro-serializer:$confluentVersion")
    implementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    implementation("org.apache.avro:avro:$avroVersion")
    implementation("org.slf4j:slf4j-simple:$slf4jVersion")

    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.25.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

avro {
    // String rather than CharSequence in the generated types
    setStringType("String")
    setFieldVisibility("PRIVATE")
}

// The .avsc files ship inside the jar too, so Feeder can load a schema by topic
// name and build records generically instead of knowing every record type.
sourceSets {
    main {
        resources.srcDir("src/main/avro")
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}

tasks.shadowJar {
    archiveFileName.set("app-all.jar")
    // Deliberately no Main-Class in the manifest: BetFeeder, MatchEventFeeder and
    // StreamsApp are all entry points, each invoked with `java -cp app-all.jar <class>`.
    mergeServiceFiles()
}

tasks.build {
    dependsOn(tasks.shadowJar)
}
