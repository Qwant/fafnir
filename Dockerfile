FROM rust:1-stretch as builder

WORKDIR /srv/fafnir

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        make \
        libgeos-c1v5 \
        libgeos-dev \
        libssl-dev \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . ./

RUN cargo build --release

FROM debian:stretch-slim

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update \
    && apt-get install -y \
        libcurl3 \
        libgeos-c1v5 \
        libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /srv/fafnir/target/release/fafnir /usr/bin/fafnir

ENTRYPOINT ["fafnir"]
