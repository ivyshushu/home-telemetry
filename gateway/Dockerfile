# gateway/Dockerfile — Multi-stage build for the Rust MQTT→OTLP gateway
#
# WHY MULTI-STAGE?
# ----------------
# A single-stage build would include the full Rust toolchain (~1.5 GB) in the
# final image. Multi-stage builds let us compile in a fat builder image and then
# copy only the stripped binary into a minimal runtime image (~100 MB).
# On a Raspberry Pi 4 with limited flash storage this matters.
#
# Stage 1: rust:1.85-slim  — has rustc, cargo, and build tools
# Stage 2: debian:bookworm-slim — has only libc and ca-certificates

# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Build
# ─────────────────────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

WORKDIR /app

# Copy the dependency manifest files first.
# Docker layer caching: if Cargo.toml and Cargo.lock haven't changed, this
# layer (and the `cargo build` below) is restored from cache, skipping the
# long dependency compilation step on subsequent builds.
COPY Cargo.toml Cargo.lock* ./

# Install system libraries required by some Rust crates:
#   pkg-config   — lets cargo find system libraries by name
#   libssl-dev   — openssl headers needed by tonic/hyper for TLS support
#                  (used when the OTel Collector endpoint is HTTPS)
RUN apt-get update && apt-get install -y pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy source and compile the release binary.
# --release enables full optimizations (LTO, dead-code elimination).
# The resulting binary is at /app/target/release/gateway.
COPY src ./src
RUN cargo build --release

# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Runtime
# ─────────────────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# ca-certificates: the TLS certificate bundle needed by tonic when connecting
# to an HTTPS OTLP endpoint (e.g. a managed OTel Collector in production).
# Even though our dev Collector uses plain HTTP (http://otel-collector:4317),
# installing ca-certificates now means the image works in production without
# rebuilding.
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage.
COPY --from=builder /app/target/release/gateway /usr/local/bin/gateway

# Run as non-root for defense-in-depth.
# This mirrors how a real EV gateway process would run under a restricted
# system user, limiting the blast radius if the process is compromised.
RUN useradd -r -s /bin/false gateway
USER gateway

# Environment variable defaults — all overridable via docker-compose.yml or
# the host environment. The MQTT_* vars point to Mosquitto; the OTLP endpoint
# points to the OTel Collector gRPC port.
ENV MQTT_HOST=localhost \
    MQTT_PORT=1883 \
    MQTT_TOPIC=sensors/temperature \
    OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
    RUST_LOG=gateway=info,rumqttc=warn

ENTRYPOINT ["/usr/local/bin/gateway"]
