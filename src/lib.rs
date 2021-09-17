mod addresses;
mod langs;
mod lazy_es;
mod pg_poi_query;
mod pois;
pub mod utils;

use config::Config;
use elasticsearch::http::transport::Transport;
use elasticsearch::Elasticsearch;
use futures::stream::{StreamExt, TryStreamExt};
use futures::{join, try_join};
use lazy_es::LazyEs;
use log::info;
use mimir2::adapters::secondary::elasticsearch::remote::connection_pool_url;
use mimir2::common::document::ContainerDocument;
use mimir2::domain::model::index::IndexVisibility;
use mimir2::domain::ports::primary::{
    generate_index::GenerateIndex, list_documents::ListDocuments,
};
use mimir2::domain::ports::secondary::remote::Remote;
use pg_poi_query::{PoisQuery, TableQuery};
use places::poi::Poi;
use pois::IndexedPoi;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
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
    /// Elasticsearch parameters.
    #[structopt(long = "es", default_value = "http://localhost:9200/")]
    es: String,
    /// Dataset to store searchable POIs
    #[structopt(short = "d", long = "dataset")]
    dataset: String,
    /// Dataset to store non-searchable POIs
    #[structopt(long = "dataset-nosearch", default_value = "nosearch")]
    dataset_nosearch: String,
    /// Number of threads used. The default is to use the number of cpus
    #[structopt(short = "n", long = "nb-threads")]
    pub nb_threads: Option<usize>,
    /// Bounding box to filter the imported pois
    /// The format is "lat1, lon1, lat2, lon2"
    #[structopt(short = "b", long = "bounding-box")]
    bounding_box: Option<String>,
    /// Number of shards for the es index
    #[structopt(short = "s", long = "nb-shards", default_value = "1")]
    nb_shards: usize,
    /// Number of replicas for the es index
    #[structopt(short = "r", long = "nb-replicas", default_value = "1")]
    nb_replicas: usize,
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
}

pub async fn load_and_index_pois(
    client: tokio_postgres::Client,
    nb_threads: usize,
    args: Args,
) -> Result<(), mimirsbrunn2::Error> {
    let langs = &args.langs;
    let max_batch_size = args.max_query_batch_size;

    // Local Elasticsearch client
    let es = &Elasticsearch::new(
        Transport::single_node(args.es.as_str())
            .expect("failed to initialize Elasticsearch transport"),
    );

    let mimir_es = Arc::new(
        connection_pool_url(&args.es)
            .await
            .expect("failed to open connection pool to Elasticsearch")
            .conn()
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

    eprintln!("Admins loaded");

    // Build Postgres query
    // TODO: This should probably be put in a function for more readability?
    let mut query = PoisQuery::new()
        .with_table(TableQuery::new("all_pois(14)").id_column("global_id"))
        .with_table(
            TableQuery::new("osm_aerodrome_label_point")
                .override_class("'aerodrome'")
                .override_subclass("'airport'"),
        )
        .with_table(
            TableQuery::new("osm_city_point")
                .override_class("'locality'")
                .override_subclass("'hamlet'")
                .filter("name <> '' AND place='hamlet'"),
        )
        .with_table(
            TableQuery::new("osm_water_lakeline")
                .override_class("'water'")
                .override_subclass("'lake'"),
        )
        .with_table(
            TableQuery::new("osm_water_point")
                .override_class("'water'")
                .override_subclass("'water'"),
        )
        .with_table(
            TableQuery::new("osm_marine_point")
                .override_class("'water'")
                .override_subclass("place"),
        );

    if let Some(ref bbox) = args.bounding_box {
        query = query.bbox(bbox);
    }

    let stmt = client
        .prepare(&query.build())
        .await
        .expect("failed to prepare query");

    info!("Processing query results...");
    let total_nb_pois = AtomicUsize::new(0);

    let poi_index_name = &format!("{}_poi_{}", MIMIR_PREFIX, args.dataset);
    let poi_index_nosearch_name = &format!("{}_poi_{}", MIMIR_PREFIX, args.dataset_nosearch);

    // Spawn tasks that will build indexes. These tasks will provide a single
    // stream to mimirsbrunn which is built from data sent into async channels.

    let poi_index_config = Config::builder()
        .add_source(Poi::default_es_container_config())
        .set_override("container.dataset", args.dataset)
        .expect("failed to create config key container.dataset")
        .build()
        .expect("failed to build mimir config");

    let poi_index_nosearch_config = Config::builder()
        .add_source(Poi::default_es_container_config())
        .set_override("container.dataset", args.dataset_nosearch)
        .expect("failed to create config key container.dataset")
        .build()
        .expect("failed to build mimir config");

    let spawn_index_task = |config| {
        let mimir_es = mimir_es.clone();
        let (send, recv) = channel::<Poi>(CHANNEL_SIZE);

        let task = async move {
            mimir_es
                .generate_index(config, ReceiverStream::new(recv), IndexVisibility::Public)
                .await
                .map_err(Error::from)
        };

        (send, task)
    };

    let (poi_channel_search, index_search_task) = spawn_index_task(poi_index_config);
    let (poi_channel_nosearch, index_nosearch_task) = spawn_index_task(poi_index_nosearch_config);

    // Build POIs and send them to indexing tasks
    let fetch_pois_task = client
        .query_raw::<_, i32, _>(&stmt, [])
        .await?
        .try_filter_map(|row| async { Ok(IndexedPoi::from_row(row, langs)) })
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
                            Ok(indexed_poi
                                .as_ref()
                                .map_err(|err| format!("failed to fetch indexed POI: {}", err))?
                                .locate_poi(
                                    admins_geofinder,
                                    langs,
                                    poi_index_name,
                                    poi_index_nosearch_name,
                                    try_skip_reverse,
                                ))
                        })
                        .collect::<Result<_, Error>>()?;

                    // Run ES queries until all POIs are fully built
                    let pois = LazyEs::batch_make_progress_until_value(es, pois, max_batch_size)
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
                    let curr_total_nb_pois =
                        total_nb_pois.fetch_add(nb_new_poi, atomic::Ordering::Relaxed) + nb_new_poi;

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

    info!("Created index {:?} for searchable POIs", index_search);
    info!("Created index {:?} for non-searchable POIs", index_nosearch);
    info!("Total number of pois: {}", total_nb_pois.into_inner());
    Ok(())
}
