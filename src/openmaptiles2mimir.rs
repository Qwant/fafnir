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

use crate::sources::openmaptiles;
use crate::utils::{get_index_creation_date, start_postgres_session};

// Prefix to ES index names for mimirsbrunn
const MIMIR_PREFIX: &str = "munin";

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

type Error = Box<dyn std::error::Error>;

#[derive(Deserialize)]
pub struct Settings {
    pub bounding_box: Option<[f64; 4]>,
    pub langs: Vec<String>,
    pub skip_reverse: bool,
    #[serde(default = "num_cpus::get")]
    pub concurrent_blocks: usize,
    pub max_query_batch_size: usize,
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

    let fetch_pois_task = openmaptiles::fetch_and_locate_pois(
        &pg_client,
        es,
        admins_geofinder,
        poi_index_name,
        poi_index_nosearch_name,
        try_skip_reverse,
        &settings,
    )
    .await
    .try_for_each(|p| {
        let total_nb_pois = &total_nb_pois;
        let poi_channel_search = poi_channel_search.clone();
        let poi_channel_nosearch = poi_channel_nosearch.clone();

        async move {
            if p.is_searchable {
                poi_channel_search
                    .send(p.poi)
                    .await
                    .expect("failed to send search POI into channel");
            } else {
                poi_channel_nosearch
                    .send(p.poi)
                    .await
                    .expect("failed to send nosearch POI into channel");
            }

            // Log advancement
            total_nb_pois.fetch_add(1, atomic::Ordering::Relaxed);
            Ok(())
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
