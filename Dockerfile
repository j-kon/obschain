FROM rust:1.85-bookworm as builder

WORKDIR /usr/src/obschain
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/src/obschain/target/release/obschain /usr/local/bin/obschain
COPY migrations /app/migrations

ENV OBSCHAIN_HOST=0.0.0.0
ENV OBSCHAIN_PORT=8080

EXPOSE 8080
CMD ["obschain"]
