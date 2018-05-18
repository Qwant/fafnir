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
extern crate rayon;
extern crate itertools;

use rayon::prelude::*;
use itertools::Itertools;
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
    let osm_id_int = row.get::<_, i64>("osm_id");
    let pg_table = row.get::<_, String>("source");
    let osm_type = if osm_id_int < 0 {
        // Imposm uses negative osm_id for relations
        "relation"
    }
    else if pg_table.ends_with("point") {
        "node"
    }
    else {
        "way"
    };

    format!(
        "osm:{osm_type}:{id}",
        osm_type = osm_type,
        id = osm_id_int.abs()
    )
}

fn build_poi_properties(row: &Row, name: &str) -> Result<Vec<Property>, String> {
    let mut properties = row
        .get_opt::<_, HashMap<_, _>>("tags")
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

    Ok(properties)
}

fn build_poi(row: &Row, geofinder: &AdminGeoFinder, rubber: &mut Rubber) -> Option<Poi> {
    let name: String = row.get("name");
    let class: String = row.get("class");
    let lat = row.get_opt("lat")?
        .map_err(|e| warn!("impossible to get lat for {} because {}", name, e))
        .ok()?;
    let lon = row.get_opt("lon")?
        .map_err(|e| warn!("impossible to get lon for {} because {}", name, e))
        .ok()?;
    let poi_coord = Coord::new(lon, lat);
    let admins = geofinder.get(&geo::Coordinate { x: lon, y: lat });
    let poi_address = rubber
        .get_address(&poi_coord)
        .ok()
        .and_then(|addrs| addrs.into_iter().next())
        .map(|addr| addr.address().unwrap());
    if poi_address.is_none() {
        warn!("The poi {:?} doesn't have any address", name);
    }
    let zip_code = match &poi_address {
        &Some(mimir::Address::Street(ref s)) => s.zip_codes.clone(),
        &Some(mimir::Address::Addr(ref a)) => a.zip_codes.clone(),
        &_ => vec![],
    };

    Some(Poi {
        id: build_poi_id(&row),
        coord: poi_coord,
        poi_type: PoiType {
            id: class.clone(),
            name: class,
        },
        label: format_label(&admins, &name),
        administrative_regions: admins,
        properties: build_poi_properties(&row, &name).unwrap_or(vec![]),
        name: name,
        weight: 0.,
        zip_codes: zip_code,
        address: poi_address,
    })
}


pub fn load_and_index_pois(es: &String, conn: &Connection, dataset: &str) {
    let rubber = &mut mimir::rubber::Rubber::new(es);
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
        SELECT osm_id, lon, lat, class, name, tags, source, mapping_key, subclass FROM
        (
            SELECT osm_id,
                st_x(st_transform(geometry, 4326)) as lon,
                st_y(st_transform(geometry, 4326)) as lat,
                poi_class(subclass, mapping_key) AS class,
                name,
                mapping_key,
                subclass,
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
                mapping_key,
                subclass,
                tags,
                'osm_poi_polygon' as source
                FROM osm_poi_polygon WHERE name <> ''
        ) as unionall
        WHERE (unionall.mapping_key,unionall.subclass) not in (('highway','bus_stop'), ('barrier','gate'), ('amenity','waste_basket'), ('amenity','post_box'), ('tourism','information'), ('amenity','recycling'), ('barrier','lift_gate'), ('barrier','bollard'), ('barrier','cycle_barrier'), ('amenity','bicycle_rental'), ('tourism','artwork'), ('amenity','toilets'), ('leisure','playground'), ('amenity','telephone'), ('amenity','taxi'), ('leisure','pitch'), ('amenity','shelter'), ('barrier','sally_port'), ('barrier','stile'), ('amenity','ferry_terminal'), ('amenity','post_office'))",
    ).unwrap();
    let trans = conn.transaction().unwrap();

    let rows = stmt.lazy_query(&trans, &[], PG_BATCH_SIZE).unwrap();
    let poi_index = rubber.make_index(dataset).unwrap();

    let rows_chunk = rows.iterator()
        .filter_map(|r| {
            r.map_err(|r| warn!("Impossible to load the row {:?}", r))
                .ok()
        })
        .chunks(20000);

    rows_chunk
        .into_iter()
        .for_each(|chunk| {
            let rows_chunk: Vec<_> = chunk.collect();
            rows_chunk.par_chunks(500)
                .for_each(|par_chunk| {
                    let mut rub = Rubber::new(es);
                    let pois = par_chunk.into_iter().filter_map(|r| {
                        build_poi(r, &admins_geofinder, &mut rub)
                        .ok_or_else(|| warn!("Problem occurred in build_poi()"))
                        .ok()
                    });
                    let mut rub2 = Rubber::new(es);
                    match rub2.bulk_index(&poi_index, pois) {
                        Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                        Ok(nb) => info!("Nb of indexed pois: {}", nb),
                    }
                })
        });

    rubber.publish_index(dataset, poi_index).unwrap();
}
