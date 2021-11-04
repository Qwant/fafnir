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
    --mount=type=cache,target=/srv/fafnir/target  \
    cargo build --release

# Move binary out of cache
RUN mkdir bin
RUN --mount=type=cache,target=/srv/fafnir/target \
    cp /srv/fafnir/target/release/openmaptiles2mimir bin/


FROM debian:buster-slim

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive
ENV RUST_LOG "tracing=info,mimir2=info,fafnir=info"

RUN apt-get update \
    && apt-get install -y libcurl4 sqlite3 npm \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN npm install -g bunyan
RUN echo "#!/bin/bash"                                  >> /usr/bin/bunyan_formated
RUN echo "CMD=$1; shift; ARG=$@"                        >> /usr/bin/bunyan_formated
RUN echo "\$CMD --config-dir /etc/fafnir @ARG | bunyan" >> /usr/bin/bunyan_formated
RUN chmod +x /usr/bin/bunyan_formated

COPY ./config /etc/fafnir
COPY --from=builder /srv/fafnir/bin/openmaptiles2mimir /usr/bin/

ENTRYPOINT ["bunyan_formated"]
