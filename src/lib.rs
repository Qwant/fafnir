extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate geojson;
extern crate itertools;
extern crate num_cpus;
extern crate par_map;

mod addresses;
mod pois;
use crate::par_map::ParMap;
use pois::IndexedPoi;

use itertools::process_results;
use mimir::rubber::{IndexSettings, IndexVisibility, Rubber};
use mimir::Poi;
use postgres::fallible_iterator::FallibleIterator;
use postgres::Client;
use std::time::Duration;

#[macro_use]
extern crate structopt;

const ES_TIMEOUT: std::time::Duration = Duration::from_secs(30);

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
}

pub fn load_and_index_pois(
    mut client: Client,
    nb_threads: usize,
    args: Args,
) -> Result<(), mimirsbrunn::Error> {
    let es = args.es.clone();
    let langs = &args.langs;
    let rubber = &mut mimir::rubber::Rubber::new(&es);
    let admins = rubber.get_all_admins().map_err(|err| {
        error!("Administratives regions not found in es db");
        err
    })?;
    let admins_geofinder = admins.into_iter().collect();

    let bbox_filter = args
        .bounding_box
        .as_ref()
        .map(|b| {
            format!(
                "WHERE ST_MakeEnvelope({}, 4326) && st_transform(geometry, 4326)",
                b
            )
        })
        .unwrap_or_else(|| "".into());

    let query = format!(
        "
        SELECT
            id,
            lon,
            lat,
            class,
            name,
            tags,
            subclass,
            mapping_key,
            poi_display_weight(name, subclass, mapping_key, tags)::float as weight
        FROM (
            SELECT
                geometry,
                global_id AS id,
                st_x(st_transform(geometry, 4326)) AS lon,
                st_y(st_transform(geometry, 4326)) AS lat,
                class,
                name,
                mapping_key,
                subclass,
                tags
            FROM all_pois(14)
            UNION ALL
            SELECT
                geometry,
                global_id_from_imposm(osm_id) AS id,
                st_x(st_transform(geometry, 4326)) AS lon,
                st_y(st_transform(geometry, 4326)) AS lat,
                'aerodrome' AS class,
                name,
                'aerodrome' AS mapping_key,
                'airport' AS subclass,
                tags
            FROM osm_aerodrome_label_point
            UNION ALL
            SELECT
                geometry,
                global_id_from_imposm(osm_id) AS id,
                st_x(st_transform(geometry, 4326)) AS lon,
                st_y(st_transform(geometry, 4326)) AS lat,
                'locality' AS class,
                name,
                'locality' AS mapping_key,
                'hamlet' AS subclass,
                tags
            FROM osm_city_point
                WHERE name <> '' AND place='hamlet'
        ) AS unionall
        {}",
        bbox_filter
    );

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

    let mut total_nb_pois = 0;
    let stmt = client.prepare(&query).expect("failed to prepare query");
    let rows_iterator = client
        .query_raw(&stmt, vec![])?
        .fuse() // Avoids consuming exhausted stream when using par_map
        .iterator();

    info!("Processing query results...");

    // "process_results" will early return on first error
    // from the postgres iterator
    process_results(rows_iterator, |rows| {
        rows.filter_map(|row| IndexedPoi::from_row(row, &langs))
            .pack(1500)
            .with_nb_threads(nb_threads)
            .par_map({
                let index = poi_index.clone();
                let index_nosearch = poi_index_nosearch.clone();
                let langs = langs.clone();
                move |p| {
                    let mut rub = Rubber::new_with_timeout(&es, ES_TIMEOUT);
                    let pois = p.into_iter().filter_map(|indexed_poi| {
                        indexed_poi.locate_poi(&admins_geofinder, &mut rub, &langs)
                    });
                    let (search, no_search): (Vec<IndexedPoi>, Vec<IndexedPoi>) =
                        pois.partition(|p| p.is_searchable);
                    let mut nb_indexed_pois = 0;
                    match rub.bulk_index(
                        &index,
                        search.into_iter().map(|indexed_poi| indexed_poi.poi),
                    ) {
                        Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                        Ok(nb) => nb_indexed_pois += nb,
                    };
                    match rub.bulk_index(
                        &index_nosearch,
                        no_search.into_iter().map(|indexed_poi| indexed_poi.poi),
                    ) {
                        Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                        Ok(nb) => nb_indexed_pois += nb,
                    };
                    nb_indexed_pois
                }
            })
            .enumerate()
            .for_each(|(i, n)| {
                total_nb_pois += n;
                let chunk_idx = i + 1;
                if chunk_idx % 100 == 0 {
                    info!(
                        "Nb of indexed pois after {} chunks: {}",
                        chunk_idx, total_nb_pois
                    );
                }
            })
    })?;

    info!("Total number of indexed pois: {}", total_nb_pois);
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
