# Fafnir

Import [imposm](https://github.com/omniscale/imposm3) POIs from PostgreSQL to [Mímirsbrunn](https://github.com/CanalTP/mimirsbrunn/) Elasticsearch 


## Build

`cargo build --release`


## Run

`./target/release/fafnir --dataset=france --es=http://localhost:9200 --pg=postgresql://localhost:5432`


## Tests

`cargo test`
