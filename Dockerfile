# Stage 1: Planner
FROM rust:1.99-bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Caching & Build
FROM chef AS builder
RUN apt-get update && apt-get install -y pkg-config libssl-dev libavahi-compat-libdnssd-dev libasound2-dev && rm -rf /var/lib/apt/lists/*
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the layer that will be cached
RUN cargo chef cook --release --recipe-path recipe.json

# Build & test application
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY openapi.json asyncapi.json ./
ARG APP_VERSION=""
ENV APP_VERSION=$APP_VERSION
RUN cargo test --release && cargo build --release --bin librespot-server

# Stage 3: Runtime
FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/slaesh/librespot-server"
LABEL org.opencontainers.image.description="Headless Spotify Connect daemon with REST API, WebSocket event stream, metadata cache, and FIFO audio sink"
LABEL org.opencontainers.image.licenses="MIT"

RUN apt-get update && apt-get install -y ca-certificates libssl-dev libavahi-compat-libdnssd1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/librespot-server /usr/local/bin/librespot-server
RUN ln -s /usr/local/bin/librespot-server /usr/local/bin/spotify-node

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["/usr/local/bin/librespot-server", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/librespot-server"]
