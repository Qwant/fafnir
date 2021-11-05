use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::TryStreamExt;
use futures::try_join;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::secondary::remote::Remote;
use mimirsbrunn::utils::logger::logger_init;
use serde::Deserialize;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use crate::mimir::{address_updated_after_pois, build_admin_geofinder, create_index, MIMIR_PREFIX};
use crate::sources::openmaptiles;
use crate::utils::start_postgres_session;

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

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

    // If addresses have not changed since last update of POIs, it is not
    // necessary to perform a reverse again for POIs that don't have an address.
    let addr_updated = address_updated_after_pois(es).await;
    let try_skip_reverse = settings.skip_reverse && !addr_updated;

    if try_skip_reverse {
        info!(
            "addresses have not been updated since last update, reverse on old POIs won't be {}",
            "performed",
        );
    }

    // Fetch admins
    let admins_geofinder = &build_admin_geofinder(mimir_es.as_ref()).await;

    // Spawn tasks that will build indexes. These tasks will provide a single
    // stream to mimirsbrunn which is built from data sent into async channels.
    let (poi_channel_search, index_search_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = create_index(
            mimir_es.as_ref(),
            &config,
            &dataset_search,
            IndexVisibility::Public,
            ReceiverStream::new(recv),
        );

        (send, task)
    };

    let (poi_channel_nosearch, index_nosearch_task) = {
        let (send, recv) = channel(CHANNEL_SIZE);

        let task = create_index(
            mimir_es.as_ref(),
            &config,
            &dataset_nosearch,
            IndexVisibility::Private,
            ReceiverStream::new(recv),
        );

        (send, task)
    };

    // Build POIs and send them to indexing tasks
    let total_nb_pois = AtomicUsize::new(0);
    let poi_index_name = &format!("{}_poi_{}", MIMIR_PREFIX, &dataset_search);
    let poi_index_nosearch_name = &format!("{}_poi_{}", MIMIR_PREFIX, &dataset_nosearch);

    let pg_client = start_postgres_session(&pg_settings.url)
        .await
        .expect("Unable to connect to postgres");

    let fetch_pois_task = {
        openmaptiles::fetch_and_locate_pois(
            &pg_client,
            es,
            admins_geofinder,
            poi_index_name,
            poi_index_nosearch_name,
            try_skip_reverse,
            &settings,
        )
        .await
        .try_for_each({
            let total_nb_pois = &total_nb_pois;

            move |p| {
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
                    // TODO: maybe we should be exhaustive as before
                    total_nb_pois.fetch_add(1, atomic::Ordering::Relaxed);
                    Ok(())
                }
            }
        })
    };

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
