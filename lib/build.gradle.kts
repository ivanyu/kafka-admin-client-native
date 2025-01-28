import org.graalvm.buildtools.gradle.tasks.BuildNativeImageTask

plugins {
    `java-library`
    id("org.graalvm.buildtools.native") version "0.10.4"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka", "kafka-clients", "3.9.0")
    compileOnly("org.graalvm.sdk", "graal-sdk", "24.0.2")
    implementation("org.slf4j", "slf4j-api", "2.0.16")
    implementation("org.slf4j", "slf4j-reload4j", "2.0.16")
//    runtimeOnly("org.slf4j", "slf4j-nop", "2.0.16")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val headersDir = file("$rootDir/lib/src/main/native")
val structsHeader = "$headersDir/libkafkaadmin_defs.h"

graalvmNative {
    toolchainDetection.set(true)

    binaries {
        named("main") {
            imageName.set("libkafkaadmin")
            sharedLibrary.set(true)
            resources.autodetect()
            buildArgs.add("-Dme.ivanyu.headerfile=$structsHeader")
            buildArgs.add("--initialize-at-build-time=" +
                    "org.apache.log4j.Logger" +
                    ",org.apache.log4j.LogManager" +
                    ",org.apache.log4j.helpers.LogLog" +
                    ",org.slf4j.LoggerFactory" +
                    ",org.apache.log4j.Layout" +
                    ",org.apache.log4j.PatternLayout" +
                    ",org.slf4j.reload4j.Reload4jLoggerFactory" +
                    ",org.slf4j.reload4j.Reload4jLoggerAdapter" +
                    ",org.slf4j.helpers.Reporter")

            javaLauncher.set(javaToolchains.launcherFor {
                languageVersion.set(JavaLanguageVersion.of(21))
                vendor.set(JvmVendorSpec.matching("Oracle Corporation"))
            })
        }
    }
}

tasks.named<BuildNativeImageTask>("nativeCompile") {
    doLast {
        copy {
            from(headersDir)
            include("*.h")
            into(outputDirectory)
        }
    }
}
