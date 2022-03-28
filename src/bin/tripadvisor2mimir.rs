use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use async_compression::tokio::bufread::GzipDecoder;
use fafnir::mimir::build_admin_geofinder;
use fafnir::sources::tripadvisor::{build_id, read_reviews};
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

    // Init Index
    let index_generator = mimir_es
        .init_container(&settings.container_tripadvisor)
        .await
        .expect("could not create index");

    // Insert POIs
    let mut indexed_documents = HashSet::new();

    let index_generator = {
        let raw_xml = read_gzip_file(&settings.tripadvisor.properties).await;
        let mut count_ok: u64 = 0;
        let mut count_errors: HashMap<_, u64> = HashMap::new();

        let pois = read_pois(raw_xml, admin_geofinder)
            .filter_map(|poi| {
                future::ready(
                    poi.map_err(|err| *count_errors.entry(err).or_insert(0) += 1)
                        .ok(),
                )
            })
            .map(|(ta_id, poi)| {
                indexed_documents.insert(ta_id);
                count_ok += 1;
                poi
            });

        let index_generator = index_generator
            .insert_documents(pois)
            .await
            .expect("could not insert POIs into index");

        info!("Parsed {} POIs", count_ok);
        info!("Skipped POIs: {:?}", count_errors);
        index_generator
    };

    // Insert Photos
    let index_generator = {
        let raw_xml = read_gzip_file(&settings.tripadvisor.photos).await;
        let mut count_ok: u64 = 0;
        let mut count_errors: HashMap<_, u64> = HashMap::new();

        let photos = read_photos(raw_xml)
            .filter_map(|photos| {
                future::ready(
                    photos
                        .map_err(|err| *count_errors.entry(err).or_insert(0) += 1)
                        .ok(),
                )
            })
            .filter(|(ta_id, _)| future::ready(indexed_documents.contains(ta_id)))
            .map(|(ta_id, url)| {
                let op = UpdateOperation::Set {
                    ident: "properties.image".to_string(),
                    value: url,
                };

                count_ok += 1;
                (build_id(ta_id), op)
            });

        let index_generator = index_generator
            .update_documents(photos)
            .await
            .expect("could not update documents from index");

        info!("Parsed {} Photos", count_ok);
        info!("Skipped Photos: {:?}", count_errors);
        index_generator
    };

    // Insert Reviews
    let index_generator = {
        let raw_xml = read_gzip_file(&settings.tripadvisor.reviews).await;
        let mut count_ok: u64 = 0;
        let mut count_errors: HashMap<_, u64> = HashMap::new();

        let reviews = read_reviews(raw_xml)
            .filter_map(|reviews| {
                future::ready(
                    reviews
                        .map_err(|err| *count_errors.entry(err).or_insert(0) += 1)
                        .ok(),
                )
            })
            .filter(|(ta_id, _)| future::ready(indexed_documents.contains(ta_id)))
            .map(|(ta_id, reviews)| {
                let op = UpdateOperation::Set {
                    ident: "reviews".to_string(),
                    value: reviews,
                };

                count_ok += 1;
                (build_id(ta_id), op)
            });

        let index_generator = index_generator
            .update_documents(reviews)
            .await
            .expect("could not update documents from index");

        info!("Parsed {} Reviews", count_ok);
        info!("Skipped Reviews: {:?}", count_errors);
        index_generator
    };

    // Publish index
    index_generator
        .publish()
        .await
        .expect("could not publish index");
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_tripadvisor).await
}
