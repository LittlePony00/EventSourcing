plugins {
    kotlin("jvm") version "1.9.0"
    kotlin("plugin.serialization") version "1.9.0"
    application
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("com.example.ApplicationKt")
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("org.postgresql:postgresql:42.7.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.0")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("org.testcontainers:testcontainers:1.19.6")
    testImplementation("org.testcontainers:kafka:1.19.6")
    testImplementation("org.testcontainers:postgresql:1.19.6")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}