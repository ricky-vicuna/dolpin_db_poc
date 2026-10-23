FROM docker.io/library/rust:1.99.1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/perps-poc /usr/local/bin/perps-poc
COPY dolphindb /app/dolphindb
WORKDIR /app
USER 65534:65534
ENTRYPOINT ["perps-poc"]
