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

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/srv/fafnir/target        \
    cargo build --release

# Move binary out of cache
RUN mkdir bin
RUN --mount=type=cache,target=/srv/fafnir/target             \
    cp /srv/fafnir/target/release/openmaptiles2mimir bin/ && \
    cp /srv/fafnir/target/release/tripadvisor2mimir bin/


FROM debian:buster-slim

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive
ENV RUST_LOG "tracing=info,mimir=info,fafnir=info"

RUN apt-get update \
    && apt-get install -y libcurl4 sqlite3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN echo "#!/bin/bash"                          >> /usr/bin/exec_fafnir
RUN echo "CMD=\$1; shift; ARG=\$@"              >> /usr/bin/exec_fafnir
RUN echo "\$CMD --config-dir /etc/fafnir \$ARG" >> /usr/bin/exec_fafnir
RUN chmod +x /usr/bin/exec_fafnir

COPY ./config /etc/fafnir
COPY --from=builder /srv/fafnir/bin/openmaptiles2mimir /usr/bin/
COPY --from=builder /srv/fafnir/bin/tripadvisor2mimir /usr/bin/

ENTRYPOINT ["exec_fafnir"]
