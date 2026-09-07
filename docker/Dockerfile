FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pgs3 /usr/local/bin/pgs3
COPY docker/pgs3-entrypoint.sh /usr/local/bin/pgs3-entrypoint.sh
# pgs3 needs no database connection and no libpq: it reads WAL segments from a
# directory and speaks the walsender protocol itself.
ENTRYPOINT ["/usr/local/bin/pgs3-entrypoint.sh"]
