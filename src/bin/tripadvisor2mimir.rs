use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_compression::tokio::bufread::GzipDecoder;
use config::Config;
use fafnir::mimir::{build_admin_geofinder, create_index};
use futures::future;
use futures::stream::StreamExt;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::secondary::remote::Remote;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::BufReader;
use tracing::info;

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

    // Initialize stats
    // TODO: the stream requires to be static so there is no way to pass mutable reference into the
    //       stream. Watch for later versions of mimir which should not enforce to use a 'static
    //       stream: https://github.com/CanalTP/mimirsbrunn/pull/625
    //       Also, after this change AdminGeoFinder should not required to be wrapped into an Arc.
    let count_ok = Arc::new(Mutex::new(0u64));
    let count_errors = Arc::new(Mutex::new(HashMap::new()));

    let pois = {
        let count_ok = count_ok.clone();
        let count_errors = count_errors.clone();

        read_pois(raw_xml, admin_geofinder)
            .filter_map(move |poi| {
                future::ready(
                    poi.map_err(|err| {
                        *count_errors
                            .lock()
                            .expect("statistics are not available")
                            .entry(err)
                            .or_insert(0) += 1
                    })
                    .ok(),
                )
            })
            .inspect(move |_| *count_ok.lock().expect("statistics are not available") += 1)
    };

    // Index POIs
    create_index(
        &mimir_es,
        &raw_config,
        &settings.container_tripadvisor.dataset,
        IndexVisibility::Private,
        pois,
    )
    .await
    .expect("error while indexing POIs");

    info!("Indexed {} POIs", count_ok.lock().unwrap());
    info!("Skipped POIs: {:?}", count_errors.lock().unwrap());
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_tripadvisor).await
}
