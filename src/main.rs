extern crate fallible_iterator;
extern crate geo;
#[macro_use]
extern crate log;
extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;

#[macro_use]
extern crate structopt;

use std::collections::HashMap;
use postgres::{Connection, TlsMode};
use postgres::rows::Row;
use fallible_iterator::FallibleIterator;
use mimir::{Coord, Poi, PoiType, Property};
use mimir::rubber::Rubber;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::utils::format_label;
use structopt::StructOpt;

const PG_BATCH_SIZE: i32 = 5000;

fn build_poi_id(row: &Row) -> String {
    format!(
        "pg:{source}:{id}",
        source = row.get::<_, String>("source"),
        id = row.get::<_, i64>("osm_id")
    )
}

fn build_poi_properties(row: &Row) -> Result<Vec<Property>, String> {
    Ok(row.get_opt::<_, HashMap<_, _>>("tags")
        .unwrap()
        .map_err(|err| {
            warn!("Unable to get tags: {:?}", err);
            err.to_string()
        })?
        .into_iter()
        .map(|(k, v)| Property {
            key: k,
            value: v.unwrap_or("".to_string()),
        })
        .collect())
}

fn build_poi(row: Row, geofinder: &AdminGeoFinder) -> Option<Poi> {
    let name: String = row.get("name");
    let class: String = row.get("class");
    let lat = row.get_opt("lat")?
        .map_err(|e| warn!("impossible to get lat for {} because {}", name, e))
        .ok()?;
    let lon = row.get_opt("lon")?
        .map_err(|e| warn!("impossible to get lon for {} because {}", name, e))
        .ok()?;
    let admins = geofinder.get(&geo::Coordinate { x: lon, y: lat });
    Some(Poi {
        id: build_poi_id(&row),
        coord: Coord::new(lon, lat),
        poi_type: PoiType {
            id: class.clone(),
            name: class,
        },
        label: format_label(&admins, &name),
        administrative_regions: admins,
        name: name,
        weight: 0.,
        zip_codes: vec![],
        properties: build_poi_properties(&row).unwrap_or(vec![]),
        address: None,
    })
}

fn index_pois<T>(mut rubber: Rubber, dataset: &str, pois: T)
where
    T: Iterator<Item = Poi>,
{
    let poi_index = rubber.make_index(dataset).unwrap();

    match rubber.bulk_index(&poi_index, pois) {
        Err(e) => panic!("Failed to bulk insert pois because: {}", e),
        Ok(nb) => info!("Nb of indexed pois: {}", nb),
    }

    rubber.publish_index(dataset, poi_index).unwrap();
}

fn load_and_index_pois(mut rubber: Rubber, conn: &Connection, dataset: &str) {
    let admins = rubber
        .get_admins_from_dataset(dataset)
        .unwrap_or_else(|err| {
            warn!(
                "Administratives regions not found in es db for dataset {}. (error: {})",
                dataset, err
            );
            vec![]
        });
    let admins_geofinder = admins.into_iter().collect();

    let stmt = conn.prepare(
        "
        SELECT osm_id,
            st_x(st_transform(geometry, 4326)) as lon,
            st_y(st_transform(geometry, 4326)) as lat,
            poi_class(subclass, mapping_key) AS class,
            name,
            tags,
            'osm_poi_point' as source
            FROM osm_poi_point
            WHERE name <> ''
        UNION ALL
        SELECT osm_id,
            st_x(st_transform(geometry, 4326)) as lon,
            st_y(st_transform(geometry, 4326)) as lat,
            poi_class(subclass, mapping_key) AS class,
            name,
            tags,
            'osm_poi_polygon' as source
            FROM osm_poi_polygon WHERE name <> ''",
    ).unwrap();
    let trans = conn.transaction().unwrap();

    let rows = stmt.lazy_query(&trans, &[], PG_BATCH_SIZE).unwrap();

    let pois = rows.iterator()
        .filter_map(|r| {
            r.map_err(|r| warn!("Impossible to load the row {:?}", r))
                .ok()
        })
        .filter_map(|r| {
            build_poi(r, &admins_geofinder)
                .ok_or_else(|| warn!("Problem occurred in build_poi()"))
                .ok()
        });
    index_pois(rubber, dataset, pois)
}

#[derive(StructOpt, Debug)]
struct Args {
    /// Postgresql parameters
    #[structopt(long = "pg")]
    pg: String,
    /// Elasticsearch parameters.
    #[structopt(long = "connection-string", default_value = "http://localhost:9200/")]
    connection_string: String,
    /// Name of the dataset.
    #[structopt(short = "d", long = "dataset")]
    dataset: String,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let conn = Connection::connect(args.pg, TlsMode::None).unwrap_or_else(|err| {
        panic!("Unable to connect to postgres: {}", err);
    });

    let rubber = Rubber::new(&args.connection_string);
    let dataset = &args.dataset;
    load_and_index_pois(rubber, &conn, dataset);
    Ok(())
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
