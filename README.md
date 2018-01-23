# Fafnir

Import [imposm](https://github.com/omniscale/imposm3) POIs from PostgreSQL to [Mímirsbrunn](https://github.com/CanalTP/mimirsbrunn/) Elasticsearch 


## Build

`cargo build --release`


## Run

`./target/release/fafnir --connection-string=http://localhost:9200 --dataset=france --pg=postgresql://localhost:5432`
