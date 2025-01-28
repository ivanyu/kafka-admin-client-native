all: lib/build/native/nativeCompile/libkafkaadmin.so

lib/build/native/nativeCompile/libkafkaadmin.so: $(shell find lib/src -type f) lib/build.gradle.kts settings.gradle.kts
	./gradlew nativeCompile --no-configuration-cache

.PHONY: test
test: lib/build/native/nativeCompile/libkafkaadmin.so
	cd tests && LD_LIBRARY_PATH=../lib/build/native/nativeCompile/ cargo test
