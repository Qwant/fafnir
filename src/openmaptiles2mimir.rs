use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::{StreamExt, TryStreamExt};
use futures::{join, try_join};
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::common::document::ContainerDocument;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::primary::{
    generate_index::GenerateIndex, list_documents::ListDocuments,
};
use mimir2::domain::ports::secondary::remote::Remote;
use mimirsbrunn::utils::logger::logger_init;
use places::poi::Poi;
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::lazy_es::LazyEs;
use crate::pois::IndexedPoi;
use crate::postgres;
use crate::utils::{get_index_creation_date, start_postgres_session};

// Prefix to ES index names for mimirsbrunn
const MIMIR_PREFIX: &str = "munin";

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

type Error = Box<dyn std::error::Error>;

#[derive(Deserialize)]
struct Settings {
    bounding_box: Option<[f64; 4]>,
    langs: Vec<String>,
    skip_reverse: bool,
    #[serde(default = "num_cpus::get")]
    concurrent_blocks: usize,
    max_query_batch_size: usize,
}

#[derive(Deserialize)]
struct PgSettings {
    url: String,
}

pub async fn load_and_index_pois(config: Config) -> Result<(), mimirsbrunn::Error> {
    // Read fafnir settings
    let settings: Settings = config.get("fafnir").expect("invalid fafnir config");
    let pg_settings: PgSettings = config.get("postgres").expect("invalid postgres config");

    let es_config: ElasticsearchStorageConfig = config
        .get("elasticsearch")
        .expect("invalid elasticsearch config");

    let dataset_search: String = config
        .get("container-search.dataset")
        .expect("could not fetch search container dataset");

    let dataset_nosearch: String = config
        .get("container-nosearch.dataset")
        .expect("could not fetch nosearch container dataset");

    // Init global logger
    let _guard = logger_init(
        config
            .get::<PathBuf>("logging.path")
            .expect("could not fetch logging path"),
    )
    .expect("could not init logger");

    info!(
        "Full configuration:\n{}",
        serde_json::to_string_pretty(
            &config
                .clone()
                .try_into::<serde_json::Value>()
                .expect("could not convert config to json"),
        )
        .expect("could not serialize config"),
    );

    // Local Elasticsearch client
    let es = &Elasticsearch::new(
        Transport::single_node(es_config.url.as_str())
            .expect("failed to initialize Elasticsearch transport"),
    );

    let mimir_es = Arc::new(
        connection_pool_url(&es_config.url)
            .conn(es_config)
            .await
            .expect("failed to open Elasticsearch connection"),
    );

    // Check if addresses have been updated more recently than last update
    let (poi_creation_date, addr_creation_date) = join!(
        get_index_creation_date(es, format!("{}_poi", MIMIR_PREFIX)),
        get_index_creation_date(es, format!("{}_addr", MIMIR_PREFIX))
    );

    let addr_updated = match (poi_creation_date, addr_creation_date) {
        (Some(poi_ts), Some(addr_ts)) => addr_ts > poi_ts,
        _ => true,
    };

    // If addresses have not changed since last update of POIs, it is not
    // necessary to perform a reverse again for POIs that don't have an address.
    let try_skip_reverse = settings.skip_reverse && !addr_updated;

    if try_skip_reverse {
        info!(
            "addresses have not been updated since last update, reverse on old POIs won't be {}",
            "performed",
        );
    }

    // Fetch administrative regions
    let admins_geofinder = &mimir_es
        .list_documents()
        .await
        .expect("administratives regions not found in es db")
        .map(|admin| admin.expect("could not parse admin"))
        .collect()
        .await;

    // Spawn tasks that will build indexes. These tasks will provide a single
    // stream to mimirsbrunn which is built from data sent into async channels.
    let spawn_index_task = |config, visibility| {
        let mimir_es = mimir_es.clone();
        let (send, recv) = channel::<Poi>(CHANNEL_SIZE);

        let task = async move {
            mimir_es
                .generate_index(config, ReceiverStream::new(recv), visibility)
                .await
                .map_err(Error::from)
        };

        (send, task)
    };

    let poi_index_config = Config::builder()
        .add_source(Poi::default_es_container_config())
        .add_source(config.clone())
        .set_override("container.dataset", dataset_search.clone())
        .expect("failed to create config key container.dataset")
        .build()
        .expect("could not build search config");

    let poi_index_nosearch_config = Config::builder()
        .add_source(Poi::default_es_container_config())
        .add_source(config.clone())
        .set_override("container.dataset", dataset_nosearch.clone())
        .expect("failed to create config key container.dataset")
        .build()
        .expect("could not build nosearch config");

    let (poi_channel_search, index_search_task) =
        spawn_index_task(poi_index_config, IndexVisibility::Public);

    let (poi_channel_nosearch, index_nosearch_task) =
        spawn_index_task(poi_index_nosearch_config, IndexVisibility::Private);

    // Build POIs and send them to indexing tasks
    let total_nb_pois = AtomicUsize::new(0);
    let poi_index_name = &format!("{}_poi_{}", MIMIR_PREFIX, &dataset_search);
    let poi_index_nosearch_name = &format!("{}_poi_{}", MIMIR_PREFIX, &dataset_nosearch);

    let pg_client = start_postgres_session(&pg_settings.url)
        .await
        .expect("Unable to connect to postgres");

    let fetch_pois_task =
        postgres::fetch_all_pois(&pg_client, settings.bounding_box, &settings.langs)
            .await
            .chunks(1500)
            .enumerate()
            .map(Ok)
            .try_for_each_concurrent(settings.concurrent_blocks, {
                let total_nb_pois = &total_nb_pois;
                let langs = &settings.langs;

                move |(chunk_idx, rows)| {
                    let poi_channel_search = poi_channel_search.clone();
                    let poi_channel_nosearch = poi_channel_nosearch.clone();

                    async move {
                        // Build POIs from postgres
                        let pois: Vec<_> = rows
                            .iter()
                            .map(|indexed_poi| {
                                Ok(indexed_poi.locate_poi(
                                    admins_geofinder,
                                    langs,
                                    poi_index_name,
                                    poi_index_nosearch_name,
                                    try_skip_reverse,
                                ))
                            })
                            .collect::<Result<_, Error>>()?;

                        // Run ES queries until all POIs are fully built
                        let pois = LazyEs::batch_make_progress_until_value(
                            es,
                            pois,
                            settings.max_query_batch_size,
                        )
                        .await
                        .into_iter()
                        .flatten();

                        // Split searchable and non-searchable POIs
                        let (search, nosearch): (Vec<IndexedPoi>, Vec<IndexedPoi>) =
                            pois.partition(|p| p.is_searchable);

                        let nb_new_poi = search.len() + nosearch.len();

                        // Send POIs to the indexing tasks
                        for IndexedPoi { poi, .. } in search {
                            poi_channel_search
                                .send(poi)
                                .await
                                .expect("failed to send search POI into channel");
                        }

                        for IndexedPoi { poi, .. } in nosearch {
                            poi_channel_nosearch
                                .send(poi)
                                .await
                                .expect("failed to send nosearch POI into channel");
                        }

                        // Log advancement
                        let curr_total_nb_pois = total_nb_pois
                            .fetch_add(nb_new_poi, atomic::Ordering::Relaxed)
                            + nb_new_poi;

                        if (chunk_idx + 1) % 100 == 0 {
                            info!(
                                "Nb of indexed pois after {} chunks: {}",
                                chunk_idx, curr_total_nb_pois,
                            );
                        }

                        Ok::<_, Error>(())
                    }
                }
            });

    // Wait for the indexing tasks to complete
    let (index_search, index_nosearch, _) =
        try_join!(index_search_task, index_nosearch_task, fetch_pois_task)
            .expect("failed to index POIs");

    let total_nb_pois = total_nb_pois.into_inner();
    info!("Created index {:?} for searchable POIs", index_search);
    info!("Created index {:?} for non-searchable POIs", index_nosearch);
    info!("Total number of pois: {}", total_nb_pois);
    Ok(())
}
