# Stage 1: Build
FROM rust:1.83-slim-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/ket /usr/local/bin/ket

ENV KET_HOME=/data/.ket
VOLUME /data

ENTRYPOINT ["ket"]
