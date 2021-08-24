mod addresses;
mod langs;
mod lazy_es;
mod pg_poi_query;
mod pois;
pub mod utils;
use lazy_es::LazyEs;
use pois::IndexedPoi;

use futures::stream::{StreamExt, TryStreamExt};
use log::{error, info};
use mimir::rubber::{IndexSettings, IndexVisibility, Rubber};
use mimir::Poi;
use reqwest::Url;
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use utils::get_index_creation_date;

#[macro_use]
extern crate structopt;

const ES_TIMEOUT: std::time::Duration = Duration::from_secs(30);

// Prefix to ES index names for mimirsbrunn
const MIMIR_PREFIX: &str = "munin";

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
) -> Result<(), mimirsbrunn::Error> {
    let es = args.es.clone();
    let es_url = &Url::parse(&es).expect("invalid ES url");
    let langs = &args.langs;
    let rubber = &mut mimir::rubber::Rubber::new(&es);
    let max_batch_size = args.max_query_batch_size;

    let poi_creation_date = get_index_creation_date(rubber, &format!("{}_poi", MIMIR_PREFIX));
    let addr_creation_date = get_index_creation_date(rubber, &format!("{}_addr", MIMIR_PREFIX));

    let addr_updated = match (poi_creation_date, addr_creation_date) {
        (Some(poi_ts), Some(addr_ts)) => addr_ts > poi_ts,
        _ => true,
    };
    let try_skip_reverse = !args.no_skip_reverse && !addr_updated;
    if try_skip_reverse {
        info!("addresses have not been updated since last update, reverse on old POIs won't be performed");
    }

    let admins = rubber.get_all_admins().map_err(|err| {
        error!("Administratives regions not found in es db");
        err
    })?;
    let admins_geofinder = &admins.into_iter().collect();

    use pg_poi_query::{PoisQuery, TableQuery};

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

    let index_settings = IndexSettings {
        nb_shards: args.nb_shards,
        nb_replicas: args.nb_replicas,
    };

    rubber.initialize_templates()?;
    let poi_index: mimir::rubber::TypedIndex<Poi> = rubber
        .make_index(&args.dataset, &index_settings)
        .expect("failed to make index");
    let poi_index_nosearch: mimir::rubber::TypedIndex<Poi> = rubber
        .make_index(&args.dataset_nosearch, &index_settings)
        .expect("failed to make index");

    let total_nb_pois = &AtomicUsize::new(0);

    let stmt = client
        .prepare(&query.build())
        .await
        .expect("failed to prepare query");

    info!("Processing query results...");

    let poi_index_name = &format!("{}_poi_{}", MIMIR_PREFIX, args.dataset);
    let poi_index_nosearch_name = &format!("{}_poi_{}", MIMIR_PREFIX, args.dataset_nosearch);

    client
        .query_raw::<_, i32, _>(&stmt, [])
        .await?
        .try_filter_map(|row| async { Ok(IndexedPoi::from_row(row, langs)) })
        .chunks(1500)
        .enumerate()
        .map(Ok)
        .try_for_each_concurrent(nb_threads, |(chunk_idx, rows)| {
            let mut rub = Rubber::new_with_timeout(&es, ES_TIMEOUT);
            let index = &poi_index;
            let index_nosearch = &poi_index_nosearch;

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
                    .collect::<Result<_, String>>()?;

                // Run ES queries until all POIs are fully built
                let pois = LazyEs::batch_make_progress_until_value(es_url, pois, max_batch_size)
                    .into_iter()
                    .flatten();

                // Split searchable and non-searchable POIs
                let (search, no_search): (Vec<IndexedPoi>, Vec<IndexedPoi>) =
                    pois.partition(|p| p.is_searchable);

                // Bulk index new POIs
                let nb_search_poi = rub
                    .bulk_index(index, search.into_iter().map(|indexed_poi| indexed_poi.poi))
                    .unwrap_or_else(|err| panic!("Failed to bulk insert pois because: {}", err));

                let nb_nosearch_poi = rub
                    .bulk_index(
                        index_nosearch,
                        no_search.into_iter().map(|indexed_poi| indexed_poi.poi),
                    )
                    .unwrap_or_else(|err| panic!("Failed to bulk insert pois because: {}", err));

                // Log advancement
                total_nb_pois.fetch_add(nb_search_poi + nb_nosearch_poi, atomic::Ordering::Relaxed);

                if (chunk_idx + 1) % 100 == 0 {
                    info!(
                        "Nb of indexed pois after {} chunks: {}",
                        chunk_idx,
                        total_nb_pois.load(atomic::Ordering::Relaxed)
                    );
                }

                Ok::<_, String>(())
            }
        })
        .await
        .expect("failed to index POIs");

    info!(
        "Total number of indexed pois: {}",
        total_nb_pois.load(atomic::Ordering::Relaxed)
    );

    rubber
        .publish_index(&args.dataset, poi_index, IndexVisibility::Public)
        .expect("failed to publish public index");
    rubber
        .publish_index(
            &args.dataset_nosearch,
            poi_index_nosearch,
            IndexVisibility::Private,
        )
        .expect("failed to publish private index");
    Ok(())
}
