extern crate fallible_iterator;
extern crate geo;
extern crate log;
extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;

use fallible_iterator::FallibleIterator;
use mimir::rubber::Rubber;
use mimir::{Coord, Poi, PoiType, Property};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::utils::format_label;
use postgres::rows::Row;
use postgres::Connection;
use std::collections::HashMap;

const PG_BATCH_SIZE: i32 = 5000;

fn build_poi_id(row: &Row) -> String {
    format!(
        "pg:{source}:{id}",
        source = row.get::<_, String>("source"),
        id = row.get::<_, i64>("osm_id")
    )
}

fn build_poi_properties(row: &Row, name: &str, rank: &i32) -> Result<Vec<Property>, String> {
    let mut properties = row.get_opt::<_, HashMap<_, _>>("tags")
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
        .collect::<Vec<Property>>();

    let poi_subclass = row.get_opt("subclass").unwrap().map_err(|e| {
        warn!("impossible to get poi_subclass for {} because {}", name, e);
        e.to_string()
    })?;

    let poi_class = row.get_opt("class").unwrap().map_err(|e| {
        warn!("impossible to get poi_class for {} because {}", name, e);
        e.to_string()
    })?;

    properties.push(Property {
        key: "poi_subclass".to_string(),
        value: poi_subclass,
    });

    properties.push(Property {
        key: "poi_class".to_string(),
        value: poi_class,
    });

    properties.push(Property {
        key: "poi_weight".to_string(),
        value: rank.to_string(),
    });

    Ok(properties)
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
    let rank = row.get_opt::<_, i32>("rank")?
        .map_err(|e| warn!("impossible to get rank for {} because {}", name, e))
        .ok()?;
    Some(Poi {
        id: build_poi_id(&row),
        coord: Coord::new(lon, lat),
        poi_type: PoiType {
            id: class.clone(),
            name: class,
        },
        label: format_label(&admins, &name),
        administrative_regions: admins,
        properties: build_poi_properties(&row, &name, &rank).unwrap_or(vec![]),
        name: name,
        weight: 0.,
        zip_codes: vec![],
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

pub fn load_and_index_pois(mut rubber: Rubber, conn: &Connection, dataset: &str) {
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
        SELECT osm_id, lon, lat, class, name, tags, source, mapping_key, subclass, rank FROM
        (
            SELECT osm_id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                poi_class(subclass, mapping_key) AS class,
                name,
                mapping_key,
                subclass,
                tags,
                'osm_poi_point' as source,
                poi_class_rank(poi_class(subclass, mapping_key)) as rank
                FROM osm_poi_point
                WHERE name <> ''
            UNION ALL
            SELECT osm_id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                poi_class(subclass, mapping_key) AS class,
                name,
                mapping_key,
                subclass,
                tags,
                'osm_poi_polygon' as source,
                poi_class_rank(poi_class(subclass, mapping_key)) as rank
                FROM osm_poi_polygon WHERE name <> ''
        ) as unionall
        WHERE (unionall.mapping_key,unionall.subclass) not in (('highway','bus_stop'), ('barrier','gate'), ('amenity','waste_basket'), ('amenity','post_box'), ('tourism','information'), ('amenity','recycling'), ('barrier','lift_gate'), ('barrier','bollard'), ('barrier','cycle_barrier'), ('amenity','bicycle_rental'), ('tourism','artwork'), ('amenity','toilets'), ('leisure','playground'), ('amenity','telephone'), ('amenity','taxi'), ('leisure','pitch'), ('amenity','shelter'), ('barrier','sally_port'), ('barrier','stile'), ('amenity','ferry_terminal'), ('amenity','post_office'))",
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
