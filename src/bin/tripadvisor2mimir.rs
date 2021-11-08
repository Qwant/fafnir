use std::path::PathBuf;
use std::sync::Arc;

use async_compression::tokio::bufread::GzipDecoder;
use config::Config;
use fafnir::mimir::{build_admin_geofinder, create_index};
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::secondary::remote::Remote;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::BufReader;

use fafnir::settings::{ContainerConfig, FafnirSettings};
use fafnir::sources::tripadvisor::read_pois;

#[derive(Debug, Deserialize)]
struct TripAdvisorSettings {
    properties: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Settings {
    tripadvisor: TripAdvisorSettings,
    fafnir: FafnirSettings,
    elasticsearch: ElasticsearchStorageConfig,
    container_tripadvisor: ContainerConfig,
}

async fn load_and_index_tripadvisor(settings: Settings, raw_config: Config) {
    // Open GZip file
    let file = File::open(&settings.tripadvisor.properties)
        .await
        .expect("could not open input");

    let raw_xml = BufReader::new(GzipDecoder::new(BufReader::new(file)));

    // Connect to mimir ES
    let mimir_es = connection_pool_url(&settings.elasticsearch.url)
        .conn(settings.elasticsearch)
        .await
        .expect("failed to open Elasticsearch connection");

    let admin_geofinder = Arc::new(build_admin_geofinder(&mimir_es).await);

    create_index(
        &mimir_es,
        &raw_config,
        &settings.container_tripadvisor.dataset,
        IndexVisibility::Private,
        read_pois(raw_xml, admin_geofinder),
    )
    .await
    .expect("error while indexing POIs");
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_tripadvisor).await
}
