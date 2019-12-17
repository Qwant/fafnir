extern crate fallible_iterator;
extern crate log;
extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate itertools;
extern crate num_cpus;
extern crate par_map;

use fallible_iterator::FallibleIterator;
use itertools::process_results;
use mimir::rubber::{IndexSettings, IndexVisibility, Rubber};
use mimir::{Coord, Poi, PoiType, Property};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_international_poi_label;
use mimirsbrunn::labels::{format_addr_name_and_label, format_poi_label, format_street_label};
use mimirsbrunn::utils::find_country_codes;
use std::ops::Deref;

use par_map::ParMap;
use postgres::rows::Row;
use postgres::Connection;
use std::collections::HashMap;
use std::sync::Arc;

const PG_BATCH_SIZE: i32 = 5000;

fn properties_from_row(row: &Row) -> Result<Vec<Property>, String> {
    let properties = row
        .get_opt::<_, HashMap<_, _>>("tags")
        .unwrap()
        .map_err(|err| {
            warn!("Unable to get tags: {:?}", err);
            err.to_string()
        })?
        .into_iter()
        .map(|(k, v)| Property {
            key: k,
            value: v.unwrap_or_else(|| "".to_string()),
        })
        .collect::<Vec<Property>>();

    Ok(properties)
}

fn build_names(langs: &[String], properties: &[Property]) -> Result<mimir::I18nProperties, String> {
    const NAME_TAG_PREFIX: &str = "name:";

    let properties = properties
        .iter()
        .filter_map(|property| {
            if property.key.starts_with(&NAME_TAG_PREFIX) {
                let lang = property.key[NAME_TAG_PREFIX.len()..].to_string();
                if langs.contains(&lang) {
                    Some(mimir::Property {
                        key: lang,
                        value: property.value.to_string(),
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    Ok(mimir::I18nProperties(properties))
}

fn build_poi_properties(
    row: &Row,
    name: &str,
    mut properties: Vec<Property>,
) -> Result<Vec<Property>, String> {
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

fn iter_admins(admins: &[Arc<mimir::Admin>]) -> impl Iterator<Item = &mimir::Admin> + Clone {
    admins.iter().map(|a| a.deref())
}

/// Read the osm address tags and build a mimir address from them
///
/// For the moment we read mostly `addr:city` or `addr:country`
/// if available we also read `addr:postcode`
///
/// We also search for the admins that contains the coordinates of the poi
/// and add them as the address's admins.
///
/// For the moment we do not read `addr:city` or `addr:country` as it could
/// lead to inconsistency with the admins hierarchy
fn build_new_addr(
    addr_tag: &str,
    street_tag: &str,
    poi: &Poi,
    admins: Vec<Arc<mimir::Admin>>,
) -> mimir::Address {
    let postcode = poi
        .properties
        .iter()
        .find(|p| &p.key == "addr:postcode")
        .map(|p| p.value.clone());
    let postcodes = postcode.map_or(vec![], |p| vec![p]);
    let country_codes = find_country_codes(iter_admins(&admins));
    let street_label = format_street_label(street_tag, iter_admins(&admins), &country_codes);
    let (addr_name, addr_label) =
        format_addr_name_and_label(addr_tag, street_tag, iter_admins(&admins), &country_codes);
    let weight = admins.iter().find(|a| a.is_city()).map_or(0., |a| a.weight);
    mimir::Address::Addr(mimir::Addr {
        id: format!("addr_poi:{}", &poi.id),
        house_number: addr_tag.into(),
        name: addr_name,
        street: mimir::Street {
            id: format!("street_poi:{}", &poi.id),
            name: street_tag.to_string(),
            label: street_label,
            administrative_regions: admins,
            weight: weight,
            zip_codes: postcodes.clone(),
            coord: poi.coord,
            country_codes: country_codes.clone(),
            ..Default::default()
        },
        label: addr_label,
        coord: poi.coord,
        approx_coord: None,
        weight: weight,
        zip_codes: postcodes,
        distance: None,
        country_codes,
    })
}

fn find_address(
    poi: &Poi,
    geofinder: &AdminGeoFinder,
    rubber: &mut Rubber,
) -> Option<mimir::Address> {
    if poi
        .properties
        .iter()
        .any(|p| p.key == "poi_class" && p.value == "locality")
    {
        // We don't want to add address on hamlets.
        return None;
    }
    let osm_addr_tag = ["addr:housenumber", "contact:housenumber"]
        .iter()
        .filter_map(|k| {
            poi.properties
                .iter()
                .find(|p| &p.key == k)
                .map(|p| &p.value)
        })
        .next();

    let osm_street_tag = ["addr:street", "contact:street"]
        .iter()
        .filter_map(|k| {
            poi.properties
                .iter()
                .find(|p| &p.key == k)
                .map(|p| &p.value)
        })
        .next();

    match (osm_addr_tag, osm_street_tag) {
        (Some(addr_tag), Some(street_tag)) => Some(build_new_addr(
            addr_tag,
            street_tag,
            poi,
            geofinder.get(&poi.coord),
        )),
        _ => rubber
            .get_address(&poi.coord)
            .ok()
            .and_then(|addrs| addrs.into_iter().next())
            .map(|addr| addr.address().unwrap()),
    }
}

fn locate_poi(
    mut poi: Poi,
    geofinder: &AdminGeoFinder,
    rubber: &mut Rubber,
    langs: &[String],
) -> Option<Poi> {
    let poi_address = find_address(&poi, geofinder, rubber);

    // if we have an address, we take the address's admin as the poi's admin
    // else we lookup the admin by the poi's coordinates
    let (admins, country_codes) = poi_address
        .as_ref()
        .map(|a| match a {
            mimir::Address::Street(ref s) => {
                (s.administrative_regions.clone(), s.country_codes.clone())
            }
            mimir::Address::Addr(ref s) => (
                s.street.administrative_regions.clone(),
                s.country_codes.clone(),
            ),
        })
        .unwrap_or_else(|| {
            let admins = geofinder.get(&poi.coord);
            let country_codes = find_country_codes(iter_admins(&admins));
            (admins, country_codes)
        });

    if admins.is_empty() {
        debug!("The poi {} is not on any admins", &poi.name);
        return None;
    }

    if poi_address.is_none() {
        debug!(
            "The poi {} doesn't have any address (admins: {:?})",
            &poi.name, &admins
        );
    }

    let zip_codes = match poi_address {
        Some(mimir::Address::Street(ref s)) => s.zip_codes.clone(),
        Some(mimir::Address::Addr(ref a)) => a.zip_codes.clone(),
        _ => vec![],
    };

    poi.administrative_regions = admins;
    poi.address = poi_address;
    poi.label = format_poi_label(
        &poi.name,
        iter_admins(&poi.administrative_regions),
        &country_codes,
    );
    poi.labels = format_international_poi_label(
        &poi.names,
        &poi.name,
        &poi.label,
        iter_admins(&poi.administrative_regions),
        &country_codes,
        langs,
    );
    poi.zip_codes = zip_codes;
    Some(poi)
}

fn build_poi(row: Row, langs: &[String]) -> Option<Poi> {
    let id = row.get("id");
    let name: String = row.get("name");

    let class: String = row.get("class");
    let subclass: String = row.get("subclass");

    let poi_type_id: String = format!("class_{}:subclass_{}", class, subclass);
    let poi_type_name: String = format!("class_{} subclass_{}", class, subclass);

    let weight = row.get("weight");

    let lat = row
        .get_opt("lat")?
        .map_err(|e| warn!("impossible to get lat for {} because {}", name, e))
        .ok()?;
    let lon = row
        .get_opt("lon")?
        .map_err(|e| warn!("impossible to get lon for {} because {}", name, e))
        .ok()?;

    let poi_coord = Coord::new(lon, lat);

    if !poi_coord.is_valid() {
        // Ignore PoI if its coords from db are invalid.
        // Especially, NaN values may exist because of projection
        // transformations around poles.
        warn!("Got invalid coord for {} lon={},lat={}", id, lon, lat);
        return None;
    }

    let row_properties = properties_from_row(&row).unwrap_or_else(|_| vec![]);

    let names =
        build_names(langs, &row_properties).unwrap_or_else(|_| mimir::I18nProperties::default());

    let properties = build_poi_properties(&row, &name, row_properties).unwrap_or_else(|_| vec![]);

    Some(Poi {
        id: id,
        coord: poi_coord,
        poi_type: PoiType {
            id: poi_type_id,
            name: poi_type_name,
        },
        label: "".into(),
        properties: properties,
        name,
        weight,
        names,
        labels: mimir::I18nProperties::default(),
        ..Default::default()
    })
}

pub fn load_and_index_pois(
    es: String,
    conn: Connection,
    dataset: String,
    nb_threads: usize,
    bounding_box: Option<String>,
    nb_shards: usize,
    nb_replicas: usize,
    langs: Vec<String>,
) -> Result<(), mimirsbrunn::Error> {
    let rubber = &mut mimir::rubber::Rubber::new(&es);
    let admins = rubber.get_all_admins().map_err(|err| {
        error!("Administratives regions not found in es db");
        err
    })?;
    let admins_geofinder = admins.into_iter().collect();

    let bbox_filter = bounding_box
        .map(|b| {
            format!(
                "and ST_MakeEnvelope({}, 4326) && st_transform(geometry, 4326)",
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
            FROM layer_poi(NULL, 14, 1)
                WHERE name <> ''
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
                WHERE name <> ''
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
        WHERE (unionall.mapping_key,unionall.subclass) not in
            (('highway','bus_stop'),
             ('barrier','gate'),
             ('amenity','waste_basket'),
             ('amenity','post_box'),
             ('tourism','information'),
             ('amenity','recycling'),
             ('barrier','lift_gate'),
             ('barrier','bollard'),
             ('barrier','cycle_barrier'),
             ('amenity','bicycle_rental'),
             ('tourism','artwork'),
             ('amenity','toilets'),
             ('leisure','playground'),
             ('amenity','telephone'),
             ('amenity','taxi'),
             ('leisure','pitch'),
             ('amenity','shelter'),
             ('barrier','sally_port'),
             ('barrier','stile'),
             ('amenity','ferry_terminal'),
             ('amenity','post_office'),
             ('railway','subway_entrance'),
             ('railway','train_station_entrance'))
         {}",
        bbox_filter
    );

    let stmt = conn.prepare(&query).unwrap();
    let trans = conn.transaction().unwrap();

    let rows_iterator = stmt
        .lazy_query(&trans, &[], PG_BATCH_SIZE)
        .expect("failed to execute query")
        .iterator();

    let index_settings = IndexSettings {
        nb_shards: nb_shards,
        nb_replicas: nb_replicas,
    };

    rubber.initialize_templates()?;
    let poi_index = rubber.make_index(&dataset, &index_settings).unwrap();

    // "process_results" will early return on first error
    // from the postgres iterator
    process_results(rows_iterator, |rows| {
        rows.filter_map(|row| {
            build_poi(row, &langs)
                .ok_or_else(|| warn!("Problem occurred in build_poi()"))
                .ok()
        })
        .pack(1000)
        .with_nb_threads(nb_threads)
        .par_map({
            let i = poi_index.clone();
            let langs = langs.clone();
            move |p| {
                let mut rub = Rubber::new(&es);
                let pois = p
                    .into_iter()
                    .filter_map(|poi| locate_poi(poi, &admins_geofinder, &mut rub, &langs));
                let mut rub2 = Rubber::new(&es);
                match rub2.bulk_index(&i, pois) {
                    Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                    Ok(nb) => info!("Nb of indexed pois: {}", nb),
                };
            }
        })
        .for_each(|_| {})
    })?;

    rubber
        .publish_index(&dataset, poi_index, IndexVisibility::Public)
        .unwrap();
    Ok(())
}
