use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_compression::tokio::bufread::GzipDecoder;
use fafnir::mimir::build_admin_geofinder;
use futures::future;
use futures::stream::StreamExt;
use mimir::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir::domain::model::configuration::ContainerConfig;
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

    {
        let raw_xml = read_gzip_file(&settings.tripadvisor.photos).await;

        read_photos(raw_xml)
            .for_each(|photos| async move {
                if let Ok(photos) = photos {
                    let urls = photos
                        .urls
                        .into_iter()
                        .map(|url| url.replace(',', "%2C")) // see https://en.wikipedia.org/wiki/Percent-encoding#Reserved_characters
                        .collect::<Vec<_>>()
                        .join(",");

                    println!("{}: {}", photos.id, urls)
                }
            })
            .await;
    }

    let admin_geofinder = Arc::new(build_admin_geofinder(&mimir_es).await);

    // Initialize stats
    // TODO: the stream requires to be static so there is no way to pass mutable reference into the
    //       stream. Watch for later versions of mimir which should not enforce to use a 'static
    //       stream: https://github.com/CanalTP/mimirsbrunn/pull/625
    //       Also, after this change AdminGeoFinder should not required to be wrapped into an Arc.
    let count_ok = Arc::new(AtomicU64::new(0));
    let count_errors = Arc::new(Mutex::new(HashMap::new()));
    let pois = {
        let count_ok = count_ok.clone();
        let count_errors = count_errors.clone();
        let raw_xml = read_gzip_file(&settings.tripadvisor.properties).await;

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
            .inspect(move |_| {
                count_ok.fetch_add(1, Ordering::Relaxed);
            })
    };

    // Index POIs
    mimir_es
        .generate_index(&settings.container_tripadvisor, pois)
        .await
        .expect("error while indexing POIs");

    // Output statistics
    let count_ok = Arc::try_unwrap(count_ok)
        .expect("there are remaining copies of `count_ok`")
        .into_inner();

    info!("Indexed {} POIs", count_ok);
    info!("Skipped POIs: {:?}", count_errors.lock().unwrap());
}

#[tokio::main]
async fn main() {
    fafnir::cli::run(load_and_index_tripadvisor).await
}
