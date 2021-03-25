use itertools::Itertools;
use mimir::Poi;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_addr_name_and_label;
use mimirsbrunn::labels::format_street_label;
use mimirsbrunn::utils::find_country_codes;
use serde::Deserialize;
use serde_json::json;
use std::ops::Deref;
use std::sync::Arc;

use crate::lazy_es::{EsResponse, PartialResult};

// Prefixes used in ids for Address objects derived from OSM tags
const FAFNIR_ADDR_NAMESPACE: &str = "addr_poi:";
const FAFNIR_STREET_NAMESPACE: &str = "street_poi:";

/// Check if a mimir address originates from OSM data.
pub fn is_addr_derived_from_tags(addr: &mimir::Address) -> bool {
    match addr {
        mimir::Address::Addr(addr) => addr.id.starts_with(FAFNIR_ADDR_NAMESPACE),
        mimir::Address::Street(st) => st.id.starts_with(FAFNIR_STREET_NAMESPACE),
    }
}

pub enum CurPoiAddress {
    /// No address was searched yet for this POI
    NotFound,
    /// A search already was performed, but the result was empty
    None { coord: mimir::Coord },
    /// An address has already been found
    Some {
        coord: mimir::Coord,
        address: Box<mimir::Address>,
    },
}

/// Get current value of address associated with a POI in the ES database if
/// any, together with current coordinates of the POI that have been used to
/// perform a reverse
pub fn get_current_addr<'a>(poi_index: &str, osm_id: &str) -> PartialResult<'a, CurPoiAddress> {
    #[derive(Deserialize)]
    struct FetchPoi {
        coord: mimir::Coord,
        address: Option<mimir::Address>,
    }

    PartialResult::NeedEsQuery {
        header: json!({ "index": poi_index }),
        query: json!({
            "_source": ["address", "coord"],
            "query": {"terms": {"_id": [osm_id]}}
        }),
        progress: Box::new(move |es_response| {
            let es_response: EsResponse<FetchPoi> =
                serde_json::from_str(es_response).expect("failed to parse ES response");

            PartialResult::Value({
                if let Some(poi) = es_response.hits.hits.into_iter().next() {
                    let coord = poi.source.coord;

                    if let Some(address) = poi.source.address {
                        CurPoiAddress::Some {
                            coord,
                            address: Box::new(address),
                        }
                    } else {
                        CurPoiAddress::None { coord }
                    }
                } else {
                    CurPoiAddress::NotFound
                }
            })
        }),
    }
}

pub fn get_addr_from_coords<'a>(coord: &mimir::Coord) -> PartialResult<'a, Vec<mimir::Place>> {
    let indexes = mimir::rubber::get_indexes(false, &[], &[], &["house", "street"]);

    PartialResult::NeedEsQuery {
        header: json!({
            "index": indexes,
            "ignore_unavailable": true
        }),
        query: json!({
            "query": {
                "bool": {
                    "should": mimir::rubber::build_proximity_with_boost(coord, 1.),
                    "must": {
                        "geo_distance": {
                            "distance": "1km",
                            "coord": {
                                "lat": coord.lat(),
                                "lon": coord.lon()
                            }
                        }
                    }
                }
            }
        }),
        progress: Box::new(|es_response| {
            let es_response: EsResponse<serde_json::Value> =
                serde_json::from_str(es_response).expect("failed to parse ES response");

            let places = es_response.hits.hits.into_iter().map(|hit| {
                mimir::rubber::make_place(hit.doc_type, Some(Box::new(hit.source)), None)
                    .expect("could not build place for ES response")
            });

            PartialResult::Value(places.collect())
        }),
    }
}

fn build_new_addr(
    house_number_tag: &str,
    street_tag: &str,
    poi: &Poi,
    admins: Vec<Arc<mimir::Admin>>,
) -> mimir::Address {
    let postcodes = poi
        .properties
        .iter()
        .find(|p| ["addr:postcode", "contact:postcode"].contains(&p.key.as_str()))
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
            |p| vec![p.value.to_owned()],
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
        mimir::Address::Addr(mimir::Addr {
            id: format!("{}{}", FAFNIR_ADDR_NAMESPACE, &poi.id),
            house_number: house_number_tag.into(),
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
            approx_coord: poi.approx_coord.clone(),
            weight,
            zip_codes: postcodes,
            distance: None,
            country_codes,
            context: None,
        })
    } else {
        mimir::Address::Street(mimir::Street {
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
    poi_index: &'p str,
    try_skip_reverse: bool,
) -> PartialResult<'p, Option<mimir::Address>> {
    if poi
        .properties
        .iter()
        .any(|p| p.key == "poi_class" && p.value == "locality")
    {
        // We don't want to add address on hamlets.
        return PartialResult::Value(None);
    }

    let osm_addr_tag = ["addr:housenumber", "contact:housenumber"]
        .iter()
        .find_map(|k| {
            poi.properties
                .iter()
                .find(|p| &p.key == k)
                .map(|p| &p.value)
        });

    let osm_street_tag = ["addr:street", "contact:street"].iter().find_map(|k| {
        poi.properties
            .iter()
            .find(|p| &p.key == k)
            .map(|p| &p.value)
    });

    PartialResult::Value(match (osm_addr_tag, osm_street_tag) {
        (Some(house_number_tag), Some(street_tag)) => Some(build_new_addr(
            house_number_tag,
            street_tag,
            poi,
            geofinder.get(&poi.coord),
        )),
        (None, Some(street_tag)) => {
            return get_addr_from_coords(&poi.coord).map(move |places| {
                places
                    .into_iter()
                    .find_map(|p| {
                        let as_address = p.address();

                        match &as_address {
                            Some(mimir::Address::Addr(a)) if a.street.name == *street_tag => {
                                as_address
                            }
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
            });
        }
        _ => {
            let es_address = get_addr_from_coords(&poi.coord).map(|places| {
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
                return get_current_addr(poi_index, &poi.id).partial_map(move |current_address| {
                    let changed_coords = |old_coord: mimir::Coord| {
                        (old_coord.lon() - poi.coord.lon()).abs() > 1e-6
                            || (old_coord.lat() - poi.coord.lat()).abs() > 1e-6
                    };

                    match current_address {
                        CurPoiAddress::None { coord } if !changed_coords(coord) => {
                            PartialResult::Value(None)
                        }
                        CurPoiAddress::Some { coord, address }
                            if !is_addr_derived_from_tags(&address) && !changed_coords(coord) =>
                        {
                            PartialResult::Value(Some(*address))
                        }
                        _ => es_address,
                    }
                });
            } else {
                return es_address;
            }
        }
    })
}

pub fn iter_admins(admins: &[Arc<mimir::Admin>]) -> impl Iterator<Item = &mimir::Admin> + Clone {
    admins.iter().map(|a| a.deref())
}
