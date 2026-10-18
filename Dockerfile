# Stage 1: Planner
FROM rust:1.95-bookworm AS chef
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
RUN cargo test --release && cargo build --release --bin librespot-server

# Stage 3: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl-dev libavahi-compat-libdnssd1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/librespot-server /usr/local/bin/librespot-server
RUN ln -s /usr/local/bin/librespot-server /usr/local/bin/spotify-node
ENTRYPOINT ["/usr/local/bin/librespot-server"]
