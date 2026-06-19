# Stage 1: Build dependencies only (cached layer)
FROM rust:1.96-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

# Stage 2: Prepare dependency recipe (cached unless Cargo.toml/lock changes)
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 3: Build dependencies (cached unless recipe changes)
FROM chef AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake libclang-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 4: Build actual binary (only this layer rebuilds on code changes)
COPY . .
RUN cargo build --release --bin qonduit

# Stage 5: Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/qonduit /usr/local/bin/qonduit
COPY qonduit.example.toml /etc/qonduit/qonduit.toml

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
ENTRYPOINT ["qonduit"]
CMD ["--config", "/etc/qonduit/qonduit.toml"]
