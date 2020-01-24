extern crate mimir;
extern crate mimirsbrunn;
extern crate postgres;
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate itertools;
extern crate num_cpus;
extern crate par_map;

mod pois;
use pois::IndexedPoi;

use itertools::process_results;
use mimir::rubber::{IndexSettings, IndexVisibility, Rubber};
use mimir::{Coord, Poi, PoiType, Property};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_international_poi_label;
use mimirsbrunn::labels::{format_addr_name_and_label, format_poi_label, format_street_label};
use mimirsbrunn::utils::find_country_codes;
use std::ops::Deref;
use std::time::Duration;

use par_map::ParMap;
use postgres::fallible_iterator::FallibleIterator;
use postgres::row::Row;
use postgres::Client;
use std::collections::HashMap;
use std::sync::Arc;

const ES_TIMEOUT: std::time::Duration = Duration::from_secs(30);

fn properties_from_row(row: &Row) -> Result<Vec<Property>, String> {
    let properties = row
        .try_get::<_, Option<HashMap<_, _>>>("tags")
        .map_err(|err| {
            let id: String = row.get("id");
            warn!("Unable to get tags from row '{}': {:?}", id, err);
            err.to_string()
        })?
        .unwrap_or_else(HashMap::new)
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
    id: &str,
    mut properties: Vec<Property>,
) -> Result<Vec<Property>, String> {
    let poi_subclass = row.try_get("subclass").map_err(|e| {
        warn!("impossible to get poi_subclass for {} because {}", id, e);
        e.to_string()
    })?;

    let poi_class = row.try_get("class").map_err(|e| {
        warn!("impossible to get poi_class for {} because {}", id, e);
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
            weight,
            zip_codes: postcodes.clone(),
            coord: poi.coord,
            country_codes: country_codes.clone(),
            ..Default::default()
        },
        label: addr_label,
        coord: poi.coord,
        approx_coord: None,
        weight,
        zip_codes: postcodes,
        distance: None,
        country_codes,
        context: None,
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
            .map_err(|e| {
                warn!("get_address returned ES error for {}: {}", poi.id, e);
                e
            })
            .ok()
            .and_then(|addrs| addrs.into_iter().next())
            .map(|addr| {
                addr.address()
                    .expect("get_address returned a non-address object")
            }),
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
        debug!("The poi {} is not on any admins", &poi.id);
        return None;
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

fn build_poi(row: Row, langs: &[String]) -> Option<pois::IndexedPoi> {
    let id: String = row.get("id");
    let name: String = row.get("name");

    let mapping_key: String = row.get("mapping_key");
    let class: String = row.get("class");
    let subclass: String = row.get("subclass");

    let poi_type_id: String = format!("class_{}:subclass_{}", class, subclass);
    let poi_type_name: String = format!("class_{} subclass_{}", class, subclass);

    let weight = row.get("weight");

    let lat = row
        .try_get("lat")
        .map_err(|e| warn!("impossible to get lat for {} because {}", id, e))
        .ok()?;
    let lon = row
        .try_get("lon")
        .map_err(|e| warn!("impossible to get lon for {} because {}", id, e))
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

    let properties = build_poi_properties(&row, &id, row_properties).unwrap_or_else(|_| vec![]);

    let poi = Poi {
        id,
        coord: poi_coord,
        poi_type: PoiType {
            id: poi_type_id,
            name: poi_type_name,
        },
        label: "".into(),
        properties,
        name,
        weight,
        names,
        labels: mimir::I18nProperties::default(),
        ..Default::default()
    };

    let searchable = pois::is_searchable(&poi, &mapping_key, &subclass);
    Some(pois::IndexedPoi { poi, searchable })
}

pub fn load_and_index_pois(
    es: String,
    mut client: Client,
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
            FROM layer_poi(NULL, 14, 1)
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
        {}",
        bbox_filter
    );

    let index_settings = IndexSettings {
        nb_shards,
        nb_replicas,
    };

    rubber.initialize_templates()?;
    let poi_index: mimir::rubber::TypedIndex<Poi> =
        rubber.make_index(&dataset, &index_settings).unwrap();
    let poi_index_nosearch: mimir::rubber::TypedIndex<Poi> =
        rubber.make_index("nosearch", &index_settings).unwrap();

    let mut total_nb_pois = 0;
    let stmt = client.prepare(&query).unwrap();
    let rows_iterator = client
        .query_raw(&stmt, vec![])?
        .fuse() // Avoids consuming exhausted stream when using par_map
        .iterator();

    info!("Processing query results...");

    // "process_results" will early return on first error
    // from the postgres iterator
    process_results(rows_iterator, |rows| {
        rows.filter_map(|row| build_poi(row, &langs))
            .pack(2000)
            .with_nb_threads(nb_threads)
            .par_map({
                let i = poi_index.clone();
                let i_nosearch = poi_index_nosearch.clone();
                let langs = langs.clone();
                move |p| {
                    let mut rub = Rubber::new_with_timeout(&es, ES_TIMEOUT);
                    let pois = p.into_iter().filter_map(|indexed_poi| {
                        let searchable = indexed_poi.searchable;
                        let poi = indexed_poi.poi;
                        locate_poi(poi, &admins_geofinder, &mut rub, &langs)
                            .map(|poi| pois::IndexedPoi { poi, searchable })
                    });
                    let mut rub2 = Rubber::new_with_timeout(&es, ES_TIMEOUT);

                    let (search, no_search): (Vec<IndexedPoi>, Vec<IndexedPoi>) =
                        pois.partition(|p| p.searchable);
                    let mut nb_indexed_pois = 0;
                    match rub2.bulk_index(&i, search.into_iter().map(|indexed_poi| indexed_poi.poi))
                    {
                        Err(e) => panic!("Failed to bulk insert pois because: {}", e),
                        Ok(nb) => nb_indexed_pois += nb,
                    };
                    match rub2.bulk_index(
                        &i_nosearch,
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
        .publish_index(&dataset, poi_index, IndexVisibility::Public)
        .unwrap();
    rubber
        .publish_index("nosearch", poi_index_nosearch, IndexVisibility::Private)
        .unwrap();
    Ok(())
}
