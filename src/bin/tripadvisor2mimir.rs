use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_compression::tokio::bufread::GzipDecoder;
use fafnir::mimir::build_admin_geofinder;
use fafnir::sources::tripadvisor::convert::build_id;
use futures::future;
use futures::stream::StreamExt;
use mimir::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir::domain::model::configuration::ContainerConfig;
use mimir::domain::model::update::UpdateOperation;
use mimir::domain::ports::primary::generate_index::GenerateIndex;
use mimir::domain::ports::secondary::remote::Remote;
use serde::Deserialize;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, BufReader};
use tracing::info;

use fafnir::settings::FafnirSettings;
use fafnir::sources::tripadvisor::{read_photos, read_pois};

/// Buffer size use for IO over XML files
const XML_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
struct TripAdvisorSettings {
    properties: PathBuf,
    photos: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Settings {
    tripadvisor: TripAdvisorSettings,
    fafnir: FafnirSettings,
    elasticsearch: ElasticsearchStorageConfig,
    container_tripadvisor: ContainerConfig,
}

async fn read_gzip_file(path: &Path) -> impl AsyncBufRead {
    let file = File::open(path)
        .await
        .unwrap_or_else(|err| panic!("could not open `{}`: {}", path.display(), err));

    let raw = BufReader::with_capacity(XML_BUFFER_SIZE, file);
    BufReader::new(GzipDecoder::new(raw))
}

async fn load_and_index_tripadvisor(settings: Settings) {
    // Connect to mimir ES
    let mimir_es = connection_pool_url(&settings.elasticsearch.url)
        .conn(settings.elasticsearch)
        .await
        .expect("failed to open Elasticsearch connection");

    let admin_geofinder = build_admin_geofinder(&mimir_es).await;

    // Initialize POIs
    let mut count_poi_ok: u64 = 0;
    let mut count_poi_errors: HashMap<_, u64> = HashMap::new();

    let pois = {
        let raw_xml = read_gzip_file(&settings.tripadvisor.properties).await;

        read_pois(raw_xml, admin_geofinder)
            .filter_map(|poi| {
                future::ready(
                    poi.map_err(|err| *count_poi_errors.entry(err).or_insert(0) += 1)
                        .ok(),
                )
            })
            .inspect(|_| count_poi_ok += 1)
    };

    // Initialize photos
    let mut count_photos_ok: u64 = 0;
    let mut count_photos_errors: HashMap<_, u64> = HashMap::new();

    let photos = {
        let raw_xml = read_gzip_file(&settings.tripadvisor.photos).await;

        read_photos(raw_xml).filter_map(|photos| {
            future::ready(
                photos
                    .map_err(|err| *count_photos_errors.entry(err).or_insert(0) += 1)
                    .map(|photos| {
                        let op = UpdateOperation::Set {
                            ident: "properties['ta:images']".to_string(),
                            value: photos.urls.join(","),
                        };

                        count_photos_ok += 1;
                        (build_id(photos.id), op)
                    })
                    .ok(),
            )
        })
    };

    // Index POIs
    mimir_es
        .generate_and_update_index(&settings.container_tripadvisor, pois, photos)
        .await
        .expect("error while building index");

    // Output statistics
    info!("Parsed {} POIs", count_poi_ok);
    info!("Skipped POIs: {:?}", count_poi_errors);
    info!("Parsed {} Photos", count_photos_ok);
    info!("Skipped Photos: {:?}", count_photos_errors);
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_tripadvisor).await
}
