# Dockerfile — multi-stage build for the Flink jobs fat JAR
#
# Stage 1: Build the fat JAR using Maven.
# Stage 2: Produce a lean Flink runtime image with only the JAR.
#
# Multi-stage builds keep the final image small: the Maven toolchain (~600MB)
# and downloaded dependency cache (~200MB) are discarded after Stage 1. Only
# the compiled JAR (~50MB) is copied into the final Flink image.

# =============================================================================
# Stage 1: Builder — Maven compiles Java source and packages the fat JAR.
# =============================================================================
FROM maven:3.9-eclipse-temurin-11 AS builder

# WORKDIR sets the working directory for all subsequent RUN/COPY instructions.
# Using /app is a common convention for build-stage containers.
WORKDIR /app

# Copy pom.xml first and run dependency:go-offline BEFORE copying source code.
#
# Why this order?
# Docker layers are cached. If we copy source and pom.xml together, any source
# change invalidates the dependency download layer. By copying pom.xml first
# and running dependency:go-offline in a separate layer, Docker reuses the
# cached dependency layer on source-only changes — much faster rebuilds.
#
# -q = quiet: suppresses verbose Maven download logs. Remove for debugging.
COPY pom.xml .
RUN mvn dependency:go-offline -q

# Now copy the source tree. Changes here only invalidate this layer and below.
COPY src ./src

# Build the fat JAR. Flags:
#   -DskipTests  — skip unit tests during Docker build (run tests in CI separately)
#   -q           — quiet output (remove -q to see full build log if needed)
#
# The Maven Shade Plugin (configured in pom.xml) produces two JARs in target/:
#   flink-jobs-1.0-SNAPSHOT.jar                      (thin JAR — no deps)
#   flink-jobs-1.0-SNAPSHOT-jar-with-dependencies.jar (fat JAR — all deps)
# We only need the fat JAR for the Flink cluster.
RUN mvn clean package -DskipTests -q

# =============================================================================
# Stage 2: Runtime — minimal Flink image with the compiled JAR.
# =============================================================================
FROM flink:1.18-java11

# Copy the fat JAR from the builder stage into /jobs inside the Flink image.
# The docker-compose.yml mounts ./flink:/jobs:ro into the flink-jobmanager
# container — so /jobs/flink-jobs.jar is accessible via that volume mount too.
# This COPY is useful for a self-contained image (e.g. deploying to Kubernetes).
#
# The glob *-jar-with-dependencies.jar matches the shade plugin's output
# regardless of the exact version string in pom.xml.
COPY --from=builder /app/target/flink-jobs-*-jar-with-dependencies.jar /jobs/flink-jobs.jar

# No ENTRYPOINT or CMD needed: the base flink:1.18-java11 image provides the
# `flink` CLI and `jobmanager`/`taskmanager` start scripts. We use this image
# to produce the JAR artifact; the JobManager and TaskManager containers are
# started separately via docker-compose.yml using `command: jobmanager` etc.
#
# To submit the JAR after building this image:
#   docker exec flink-jobmanager flink run \
#     /jobs/flink-jobs.jar \
#     --class com.example.RawSinkJob
#
# Or use the Flink Web UI at http://localhost:8081 to upload and submit.
