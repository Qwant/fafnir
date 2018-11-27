[![AppVeyor](https://travis-ci.org/QwantResearch/fafnir.svg?branch=master)](https://github.com/QwantResearch/fafnir)
[![Docker Pulls](https://img.shields.io/docker/pulls/qwantresearch/fafnir.svg)](https://hub.docker.com/r/qwantresearch/fafnir/)
[![GitHub license](https://img.shields.io/github/license/QwantResearch/fafnir.svg)](https://github.com/QwantResearch/fafnir/blob/master/LICENSE)

# Fafnir

- Rust tool to import [imposm](https://github.com/omniscale/imposm3) points-of-interest from a PostgreSQL database into a [Mímirsbrunn](https://github.com/CanalTP/mimirsbrunn/) Elasticsearch. 
- You can run fafnir either manually or with docker.

## Getting Started

- First build fafnir with Cargo:
```shell
cargo build --release
```
- Then you can run fafnir (with the connections to postgres and elasticsearch):
```shell
cargo run --release --dataset=<dataset name> --es=http://<es-IP>:9200 --pg=postgresql://<pg-IP>:5432
```

## Run with docker :whale:

- Fafnir can be used with [docker](https://www.docker.com/) as well.
- You can either use the [fafnir docker image](https://hub.docker.com/r/qwantresearch/fafnir/)
- Or build your own image with this repo:
```shell
docker build . -t fafnir
```

## Tests

- You can run the tests than come along fafnir directly with cargo:
```shell
cargo test
```
