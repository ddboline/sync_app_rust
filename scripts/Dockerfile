FROM rust:alpine3.14 as buildstage

RUN apk add musl-dev

RUN mkdir -p /build/sync_app_rust

ADD Cargo.toml /build/sync_app_rust/
COPY src /build/sync_app_rust/src
COPY gdrive_lib /build/sync_app_rust/gdrive_lib
COPY sync_app_http /build/sync_app_rust/sync_app_http
COPY sync_app_lib /build/sync_app_rust/sync_app_lib
COPY migrations /build/sync_app_rust/migrations
COPY templates /build/sync_app_rust/templates

RUN mkdir -p /sync_app_rust && \
    cd /build/sync_app_rust && \
    cargo install --path=. --root=/sync_app_rust

FROM alpine:3.14

COPY --from=buildstage /sync_app_rust/bin/* /usr/bin/

RUN apk add openssh-client

STOPSIGNAL SIGTERM

EXPOSE 3042

CMD ["/usr/bin/sync-app-http"]
