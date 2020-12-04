FROM rust:1-buster as builder

WORKDIR /srv/fafnir

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        make \
        libssl-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . ./

RUN cargo build --release

FROM debian:buster-slim

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        libcurl4 \
        sqlite3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /srv/fafnir/target/release/fafnir /usr/bin/fafnir

ENTRYPOINT ["fafnir"]
