use crate::addresses::find_address;
use crate::addresses::iter_admins;
use crate::langs::COUNTRIES_LANGS;
use crate::lazy_es::PartialResult;
use mimir::objects::I18nProperties;
use mimir::Poi;
use mimir::Property;
use mimir::{Coord, PoiType};
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_international_poi_label;
use mimirsbrunn::labels::format_poi_label;
use mimirsbrunn::utils::find_country_codes;
use postgres::Row;
use std::collections::BTreeSet;
use std::collections::HashMap;

use once_cell::sync::Lazy;

static NON_SEARCHABLE_ITEMS: Lazy<BTreeSet<(String, String)>> = Lazy::new(|| {
    [
        /*
            List of (mapping_key, subclass)
        */
        // POIs likely to produce lots of duplicates
        ("highway", "bus_stop"),
        ("amenity", "bicycle_rental"),
        ("amenity", "car_sharing"),
        ("amenity", "ferry_terminal"),
        ("amenity", "post_office"),
        // Street furniture and minor items
        ("amenity", "post_box"),
        ("amenity", "shelter"),
        ("amenity", "taxi"),
        ("amenity", "telephone"),
        ("amenity", "toilets"),
        ("amenity", "waste_basket"),
        ("leisure", "fitness_station"),
        ("leisure", "playground"),
        ("leisure", "pitch"),
        ("tourism", "artwork"),
        ("tourism", "information"),
        // Railway sub-items
        ("railway", "subway_entrance"),
        ("railway", "train_station_entrance"),
        // Barriers
        ("barrier", "bollard"),
        ("barrier", "cycle_barrier"),
        ("barrier", "gate"),
        ("barrier", "lift_gate"),
        ("barrier", "sally_port"),
        ("barrier", "stile"),
    ]
    .iter()
    .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
    .collect()
});

#[derive(Clone)]
pub struct IndexedPoi {
    pub poi: Poi,
    pub is_searchable: bool,
}

impl IndexedPoi {
    pub fn from_row(row: Row, langs: &[String]) -> Option<IndexedPoi> {
        let id: String = row.get("id");
        let name = row.get::<_, Option<String>>("name").unwrap_or_default();

        let mapping_key: String = row.get("mapping_key");
        let class: String = row.get("class");
        let subclass = row.get::<_, Option<String>>("subclass").unwrap_or_default();

        let poi_type_id: String = format!("class_{}:subclass_{}", class, subclass);
        let poi_type_name: String = format!("class_{} subclass_{}", class, subclass);

        let weight = row.get::<_, Option<f64>>("weight").unwrap_or(0.);

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
        let names = build_names(langs, &row_properties);
        let properties = build_poi_properties(&row, row_properties);

        let is_searchable =
            !name.is_empty() && !NON_SEARCHABLE_ITEMS.contains(&(mapping_key, subclass));

        let poi = Poi {
            id,
            coord: poi_coord,
            approx_coord: Some(poi_coord.into()),
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

        Some(IndexedPoi { poi, is_searchable })
    }

    pub fn locate_poi<'a>(
        &'a self,
        geofinder: &'a AdminGeoFinder,
        langs: &'a [String],
        poi_index: &'a str,
        poi_index_nosearch: &'a str,
        try_skip_reverse: bool,
    ) -> PartialResult<'a, Option<IndexedPoi>> {
        let index = if self.is_searchable {
            poi_index
        } else {
            poi_index_nosearch
        };

        find_address(&self.poi, geofinder, index, try_skip_reverse).map(move |poi_address| {
            let mut res = self.clone();

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
                    let admins = geofinder.get(&res.poi.coord);
                    let country_codes = find_country_codes(iter_admins(&admins));
                    (admins, country_codes)
                });

            if admins.is_empty() {
                debug!("The poi {} is not on any admins", &res.poi.id);
                return None;
            }

            let zip_codes = match poi_address {
                Some(mimir::Address::Street(ref s)) => s.zip_codes.clone(),
                Some(mimir::Address::Addr(ref a)) => a.zip_codes.clone(),
                None => vec![],
            };

            res.poi.administrative_regions = admins;
            res.poi.address = poi_address;
            res.poi.label = format_poi_label(
                &res.poi.name,
                iter_admins(&res.poi.administrative_regions),
                &country_codes,
            );
            res.poi.labels = format_international_poi_label(
                &res.poi.names,
                &res.poi.name,
                &res.poi.label,
                iter_admins(&res.poi.administrative_regions),
                &country_codes,
                langs,
            );
            for country_code in country_codes.iter() {
                if let Some(country_langs) =
                    COUNTRIES_LANGS.get(country_code.to_uppercase().as_str())
                {
                    let has_lang = |props: &I18nProperties, lang: &str| {
                        props.0.iter().any(|prop| prop.key == lang)
                    };

                    for lang in country_langs {
                        if langs.contains(&lang.to_string()) && !has_lang(&res.poi.labels, lang) {
                            res.poi.labels.0.push(Property {
                                key: lang.to_string(),
                                value: res.poi.label.clone(),
                            });
                        }
                    }

                    for lang in country_langs {
                        if langs.contains(&lang.to_string()) && !has_lang(&res.poi.names, lang) {
                            res.poi.names.0.push(Property {
                                key: lang.to_string(),
                                value: res.poi.name.clone(),
                            })
                        }
                    }
                }
            }
            res.poi.zip_codes = zip_codes;
            res.poi.country_codes = country_codes;
            Some(res)
        })
    }
}

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

fn build_poi_properties(row: &Row, mut properties: Vec<Property>) -> Vec<Property> {
    if let Ok(poi_subclass) = row.try_get("subclass") {
        properties.push(Property {
            key: "poi_subclass".to_string(),
            value: poi_subclass,
        });
    };
    if let Ok(poi_class) = row.try_get("class") {
        properties.push(Property {
            key: "poi_class".to_string(),
            value: poi_class,
        });
    };
    properties
}

fn build_names(langs: &[String], properties: &[Property]) -> mimir::I18nProperties {
    const NAME_TAG_PREFIX: &str = "name:";

    let properties = properties
        .iter()
        .filter_map(|property| {
            if property.key.starts_with(&NAME_TAG_PREFIX) {
                let lang = property.key[NAME_TAG_PREFIX.len()..].to_string();
                if langs.contains(&lang) {
                    return Some(mimir::Property {
                        key: lang,
                        value: property.value.to_string(),
                    });
                }
            }
            None
        })
        .collect();

    mimir::I18nProperties(properties)
}
