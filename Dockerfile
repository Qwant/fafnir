FROM rust:1-jessie as builder

WORKDIR /srv/fafnir

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y make libgeos-c1 libssl-dev git && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . ./

RUN cargo build --release

FROM debian:jessie-slim

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y libcurl3 libgeos-c1 libssl-dev && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /srv/fafnir/target/release/fafnir /usr/bin/fafnir

ENTRYPOINT ["fafnir"]
