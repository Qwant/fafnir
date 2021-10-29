use itertools::Itertools;
use mimirsbrunn2::admin_geofinder::AdminGeoFinder;
use mimirsbrunn2::labels::{format_addr_name_and_label, format_street_label};
// use mimirsbrunn2::utils::find_country_codes;
use places::{
    addr::Addr, admin::find_country_codes, admin::Admin, coord::Coord, poi::Poi, street::Street,
    Address, Place,
};
use serde::Deserialize;
use serde_json::json;
use std::ops::Deref;
use std::sync::Arc;

use crate::lazy_es::{parse_es_response, LazyEs};

// Prefixes used in ids for Address objects derived from OSM tags
const FAFNIR_ADDR_NAMESPACE: &str = "addr_poi:";
const FAFNIR_STREET_NAMESPACE: &str = "street_poi:";
const MAX_REVERSE_DISTANCE: &str = "500m";

/// Check if a mimir address originates from OSM data.
pub fn is_addr_derived_from_tags(addr: &Address) -> bool {
    match addr {
        Address::Addr(addr) => addr.id.starts_with(FAFNIR_ADDR_NAMESPACE),
        Address::Street(st) => st.id.starts_with(FAFNIR_STREET_NAMESPACE),
    }
}

pub enum CurPoiAddress {
    /// No address was searched yet for this POI
    NotFound,
    /// A search already was performed, but the result was empty
    None { coord: Coord },
    /// An address has already been found
    Some { coord: Coord, address: Box<Address> },
}

/// Get current value of address associated with a POI in the ES database if
/// any, together with current coordinates of the POI that have been used to
/// perform a reverse
pub fn get_current_addr<'a>(poi_index: &str, osm_id: &'a str) -> LazyEs<'a, CurPoiAddress> {
    #[derive(Deserialize)]
    struct FetchPoi {
        coord: Coord,
        address: Option<Address>,
    }

    LazyEs::NeedEsQuery {
        header: json!({ "index": poi_index }),
        query: json!({
            "_source": ["address", "coord"],
            "query": {"terms": {"_id": [osm_id]}}
        }),
        progress: Box::new(move |es_response| {
            LazyEs::Value({
                let hits = parse_es_response(es_response)
                    .expect("got error from ES while reading old address");

                assert!(hits.len() <= 1);

                hits.into_iter()
                    .next()
                    .map(|hit| {
                        let poi: FetchPoi = hit.source;
                        let coord = poi.coord;

                        if let Some(address) = poi.address {
                            CurPoiAddress::Some {
                                coord,
                                address: Box::new(address),
                            }
                        } else {
                            CurPoiAddress::None { coord }
                        }
                    })
                    .unwrap_or(CurPoiAddress::NotFound)
            })
        }),
    }
}

/// Get addresses close to input coordinates.
pub fn get_addr_from_coords<'a>(coord: &Coord) -> LazyEs<'a, Vec<Place>> {
    LazyEs::NeedEsQuery {
        header: json!({
            "index": ["munin_addr"],
            "ignore_unavailable": true
        }),
        query: json!({
            "query": {
                "bool": {
                    "must": { "match_all": {} },
                    "filter": {
                        "geo_distance": {
                            "distance": MAX_REVERSE_DISTANCE,
                            "coord": { "lat": coord.lat(), "lon": coord.lon() }
                        }
                    }
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "coord": { "lat": coord.lat(), "lon": coord.lon() },
                        "order": "asc",
                        "unit": "m",
                        "distance_type": "arc",
                        "ignore_unmapped": true
                    }
                }
            ]
        }),
        progress: Box::new(|es_response| {
            LazyEs::Value(
                parse_es_response(es_response)
                    .expect("got error from ES while performing reverse")
                    .into_iter()
                    .map(|hit| Place::Addr(serde_json::from_value(hit.source).unwrap()))
                    .collect(),
            )
        }),
    }
}

fn build_new_addr(
    house_number_tag: &str,
    street_tag: &str,
    poi: &Poi,
    admins: Vec<Arc<Admin>>,
) -> Address {
    let postcodes = poi
        .properties
        .iter()
        .find(|(key, _)| ["addr:postcode", "contact:postcode"].contains(&key.as_str()))
        .map_or_else(
            || {
                admins
                    .iter()
                    .filter(|admin| admin.zip_codes.len() == 1)
                    .sorted_by_key(|admin| admin.zone_type)
                    .map(|admin| admin.zip_codes.clone())
                    .next()
                    .unwrap_or_else(Vec::new)
            },
            |(_, val)| vec![val.to_owned()],
        );
    let country_codes = find_country_codes(iter_admins(&admins));
    let street_label = format_street_label(street_tag, iter_admins(&admins), &country_codes);

    let (addr_name, addr_label) = format_addr_name_and_label(
        house_number_tag,
        street_tag,
        iter_admins(&admins),
        &country_codes,
    );
    let weight = admins.iter().find(|a| a.is_city()).map_or(0., |a| a.weight);
    if !house_number_tag.is_empty() {
        Address::Addr(Addr {
            id: format!("{}{}", FAFNIR_ADDR_NAMESPACE, &poi.id),
            house_number: house_number_tag.into(),
            name: addr_name,
            street: Street {
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
            approx_coord: poi.approx_coord.clone(),
            weight,
            zip_codes: postcodes,
            distance: None,
            country_codes,
            context: None,
        })
    } else {
        Address::Street(Street {
            id: format!("{}{}", FAFNIR_STREET_NAMESPACE, &poi.id),
            name: street_tag.to_string(),
            label: street_label,
            administrative_regions: admins,
            weight,
            zip_codes: postcodes,
            coord: poi.coord,
            country_codes,
            ..Default::default()
        })
    }
}

/// Build mimir Address from Poi,using osm address tags (if present)
/// or using reverse geocoding.
///
/// We also search for the admins that contains the coordinates of the poi
/// and add them as the address's admins.
///
/// If try_skip_reverse is set to true, it will reuse the address already
/// attached to a POI in the ES database.
pub fn find_address<'p>(
    poi: &'p Poi,
    geofinder: &'p AdminGeoFinder,
    poi_index: &str,
    try_skip_reverse: bool,
) -> LazyEs<'p, Option<Address>> {
    if poi
        .properties
        .iter()
        .any(|(key, val)| key == "poi_class" && val == "locality")
    {
        // We don't want to add address on hamlets.
        return LazyEs::Value(None);
    }

    let osm_addr_tag = ["addr:housenumber", "contact:housenumber"]
        .iter()
        .find_map(|k| {
            poi.properties
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, val)| val)
        });

    let osm_street_tag = ["addr:street", "contact:street"].iter().find_map(|k| {
        poi.properties
            .iter()
            .find(|(key, _)| key == k)
            .map(|(_, val)| val)
    });

    match (osm_addr_tag, osm_street_tag) {
        (Some(house_number_tag), Some(street_tag)) => LazyEs::Value(Some(build_new_addr(
            house_number_tag,
            street_tag,
            poi,
            geofinder.get(&poi.coord),
        ))),
        (None, Some(street_tag)) => get_addr_from_coords(&poi.coord).map(move |addrs| {
            addrs
                .into_iter()
                .find_map(|p| {
                    let as_address = p.address();

                    match &as_address {
                        Some(Address::Addr(a)) if a.street.name == *street_tag => as_address,
                        _ => None,
                    }
                })
                .or_else(|| {
                    Some(build_new_addr(
                        "",
                        street_tag,
                        poi,
                        geofinder.get(&poi.coord),
                    ))
                })
        }),
        _ => {
            let lazy_es_address = get_addr_from_coords(&poi.coord).map(|places| {
                Some(
                    places
                        .into_iter()
                        .next()?
                        .address()
                        .expect("`get_address_from_coords` returned a non-address object"),
                )
            });

            if try_skip_reverse {
                // Fetch the address already attached to the POI to avoid computing an
                // unnecessary reverse.
                get_current_addr(poi_index, &poi.id).then(move |current_address| {
                    let changed_coords = |old_coord: Coord| {
                        (old_coord.lon() - poi.coord.lon()).abs() > 1e-6
                            || (old_coord.lat() - poi.coord.lat()).abs() > 1e-6
                    };

                    match current_address {
                        CurPoiAddress::None { coord } if !changed_coords(coord) => {
                            LazyEs::Value(None)
                        }
                        CurPoiAddress::Some { coord, address }
                            if !is_addr_derived_from_tags(&address) && !changed_coords(coord) =>
                        {
                            LazyEs::Value(Some(*address))
                        }
                        _ => lazy_es_address,
                    }
                })
            } else {
                lazy_es_address
            }
        }
    }
}

pub fn iter_admins(admins: &[Arc<Admin>]) -> impl Iterator<Item = &Admin> + Clone {
    admins.iter().map(|a| a.deref())
}
