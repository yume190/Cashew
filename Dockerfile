# Bazel LAN remote cache server — Linux OCI image (Doc §18, §19).
FROM rust:1-bookworm AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Dependency layer: cached unless the manifest changes.
COPY Cargo.toml Cargo.lock ./
COPY desktop-gpui/Cargo.toml ./desktop-gpui/Cargo.toml
RUN mkdir -p src desktop-gpui/src \
    && echo 'fn main() {}' > src/main.rs \
    && echo 'fn main() {}' > desktop-gpui/src/main.rs \
    && cargo build --release --locked -p bazel-cache-server \
    && rm -rf src

COPY build.rs ./
COPY proto ./proto
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked -p bazel-cache-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /data cache \
    && mkdir -p /data && chown cache:cache /data

COPY --from=build /src/target/release/bazel-cache-server /usr/local/bin/bazel-cache-server

USER cache
VOLUME ["/data"]
EXPOSE 50051 9090
ENV BAZEL_CACHE_STORAGE_ROOT=/data \
    BAZEL_CACHE_LOGGING_FORMAT=json

ENTRYPOINT ["/usr/local/bin/bazel-cache-server"]
CMD ["serve"]
