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
extern crate itertools;
extern crate par_map;

use fallible_iterator::FallibleIterator;
use mimir::rubber::Rubber;
use mimir::{Coord, Poi, PoiType, Property};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::utils::format_label;
use par_map::ParMap;
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
    } else if pg_table.ends_with("point") {
        "node"
    } else {
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

fn locate_poi(poi: &mut Poi, geofinder: &AdminGeoFinder, rubber: &mut Rubber) {
    let admins = geofinder.get(&poi.coord);
    let poi_address = rubber
        .get_address(&poi.coord)
        .ok()
        .and_then(|addrs| addrs.into_iter().next())
        .map(|addr| addr.address().unwrap());
    if poi_address.is_none() {
        warn!("The poi {:?} doesn't have any address", &poi.name);
    }
    poi.administrative_regions = admins;
    poi.address = poi_address;
    poi.label = format_label(&poi.administrative_regions, &poi.name);
}

fn build_poi(row: Row) -> Option<Poi> {
    let name: String = row.get("name");
    let class: String = row.get("class");
    let lat = row
        .get_opt("lat")?
        .map_err(|e| warn!("impossible to get lat for {} because {}", name, e))
        .ok()?;
    let lon = row
        .get_opt("lon")?
        .map_err(|e| warn!("impossible to get lon for {} because {}", name, e))
        .ok()?;
    let poi_coord = Coord::new(lon, lat);

    Some(Poi {
        id: build_poi_id(&row),
        coord: poi_coord,
        poi_type: PoiType {
            id: class.clone(),
            name: class,
        },
        label: "".into(),
        administrative_regions: vec![],
        properties: build_poi_properties(&row, &name).unwrap_or(vec![]),
        name: name,
        weight: 0.,
        zip_codes: vec![],
        address: None,
    })
}

pub fn load_and_index_pois(es: String, conn: Connection, dataset: String) {
    let rubber = &mut mimir::rubber::Rubber::new(&es);
    let admins = rubber
        .get_admins_from_dataset(&dataset)
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
    let poi_index = rubber.make_index(&dataset).unwrap();

    rows.iterator()
        .filter_map(|r| {
            r.map_err(|r| warn!("Impossible to load the row {:?}", r))
                .ok()
        })
        .filter_map(|p| {
            build_poi(p)
                .ok_or_else(|| warn!("Problem occurred in build_poi()"))
                .ok()
        })
        .pack(1000)
        .par_map({
            let i = poi_index.clone();
            move |p| {
                let mut rub = Rubber::new(&es);
                let pois = p.into_iter().map(|mut r| {
                    locate_poi(&mut r, &admins_geofinder, &mut rub);
                    r
                });
                let mut rub2 = Rubber::new(&es);
                match rub2.bulk_index(&i, pois) {
                    Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                    Ok(nb) => info!("Nb of indexed pois: {}", nb),
                };
            }
        })
        .for_each(|_| {});

    rubber.publish_index(&dataset, poi_index).unwrap();
}
