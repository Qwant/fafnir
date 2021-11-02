mod addresses;
mod langs;
mod lazy_es;
mod pois;
mod postgres;
pub mod utils;

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::{StreamExt, TryStreamExt};
use futures::{join, try_join};
use lazy_es::LazyEs;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::adapters::secondary::elasticsearch::ElasticsearchStorageConfig;
use mimir2::common::config::config_from;
use mimir2::common::document::ContainerDocument;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::primary::{
    generate_index::GenerateIndex, list_documents::ListDocuments,
};
use mimir2::domain::ports::secondary::remote::Remote;
use mimirsbrunn::utils::logger::logger_init;
use places::poi::Poi;
use pois::IndexedPoi;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::{atomic, Arc};
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;
use utils::get_index_creation_date;

#[macro_use]
extern crate structopt;

// Prefix to ES index names for mimirsbrunn
const MIMIR_PREFIX: &str = "munin";

// Size of the buffers of POIs that have to be indexed.
const CHANNEL_SIZE: usize = 10_000;

type Error = Box<dyn std::error::Error>;

#[derive(StructOpt, Debug)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
pub struct Args {
    /// Postgresql parameters
    #[structopt(long = "pg")]
    pub pg: String,

    /// Number of threads used. The default is to use the number of cpus
    #[structopt(short = "n", long = "nb-threads")]
    pub nb_threads: Option<usize>,

    /// Bounding box to filter the imported pois
    /// The format is "lat1, lon1, lat2, lon2"
    #[structopt(short = "b", long = "bounding-box")]
    bounding_box: Option<String>,

    /// Languages codes, used to build i18n names and labels
    #[structopt(name = "lang", short, long)]
    langs: Vec<String>,

    /// Do not skip reverse when address information can be retrieved from previous data
    #[structopt(long)]
    no_skip_reverse: bool,

    /// Max number of tasks sent to ES simultaneously by each thread while searching for POI
    /// address
    #[structopt(default_value = "100")]
    max_query_batch_size: usize,

    /// Defines the run mode in {testing, dev, prod, ...}
    ///
    /// If no run mode is provided, a default behavior will be used.
    #[structopt(short = "m", long = "run-mode")]
    pub run_mode: Option<String>,

    /// Defines the config directories
    #[structopt(parse(from_os_str), short = "c", long = "config-dir")]
    pub config_dir: PathBuf,

    /// Override settings values using key=value
    #[structopt(short = "s", long = "setting")]
    pub settings: Vec<String>,
}

pub async fn load_and_index_pois(
    client: tokio_postgres::Client,
    nb_threads: usize,
    args: Args,
) -> Result<(), mimirsbrunn::Error> {
    let langs = &args.langs;
    let max_batch_size = args.max_query_batch_size;

    // Read config values
    let config = config_from(
        &args.config_dir,
        &["elasticsearch", "fafnir", "logging"],
        args.run_mode.as_deref(),
        "MIMIR",
        args.settings,
    )
    .expect("could not build fafnir config");

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
    let try_skip_reverse = !args.no_skip_reverse && !addr_updated;

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

    let fetch_pois_task =
        postgres::fetch_all_pois(&client, args.bounding_box.as_deref(), &args.langs)
            .await
            .chunks(1500)
            .enumerate()
            .map(Ok)
            .try_for_each_concurrent(nb_threads, {
                let total_nb_pois = &total_nb_pois;

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
                        let pois =
                            LazyEs::batch_make_progress_until_value(es, pois, max_batch_size)
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
