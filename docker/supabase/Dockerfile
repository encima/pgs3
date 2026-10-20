# pgs3 on top of the same supabase/postgres image the primary and replica run,
# so it gets the exact wal-g build Supabase ships (and psql, for `pgs3 init`).
ARG SUPABASE_POSTGRES=supabase/postgres:17.6.1.035

FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY sql ./sql
RUN cargo build --release --locked

FROM ${SUPABASE_POSTGRES}
COPY --from=build /src/target/release/pgs3 /usr/local/bin/pgs3
COPY docker/supabase/pgs3-entrypoint.sh /usr/local/bin/pgs3-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/pgs3-entrypoint.sh"]
