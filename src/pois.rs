use crate::addresses::find_address;
use crate::addresses::iter_admins;
use crate::langs::COUNTRIES_LANGS;
use crate::lazy_es::LazyEs;
use itertools::Itertools;
use log::{debug, warn};
use mimirsbrunn2::admin_geofinder::AdminGeoFinder;
use mimirsbrunn2::labels::{format_international_poi_label, format_poi_label};
use places::{
    admin::find_country_codes,
    coord::Coord,
    i18n_properties::I18nProperties,
    poi::{Poi, PoiType},
    Address, Property,
};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use once_cell::sync::Lazy;

const TAGS_TO_INDEX_AS_POI_TYPE_NAME: &[&str] = &["cuisine"];

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
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect()
});

#[derive(Clone)]
pub struct IndexedPoi {
    pub poi: Poi,
    pub is_searchable: bool,
}

impl IndexedPoi {
    pub fn from_row(row: tokio_postgres::Row, langs: &[String]) -> Option<IndexedPoi> {
        let id: String = row.get("id");
        let name = row.get::<_, Option<String>>("name").unwrap_or_default();

        let mapping_key: String = row.get("mapping_key");
        let class: String = row.get("class");
        let subclass = row.get::<_, Option<String>>("subclass").unwrap_or_default();
        let tags = row
            .get::<_, Option<HashMap<_, _>>>("tags")
            .unwrap_or_default();

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

        let poi_type_id = format!("class_{}:subclass_{}", class, subclass);
        let poi_type_text = build_poi_type_text(&class, &subclass, &tags);
        let row_properties = properties_from_tags(tags);
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
                name: poi_type_text,
            },
            label: "".into(),
            properties,
            name,
            weight,
            names,
            labels: I18nProperties::default(),
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
    ) -> LazyEs<'a, Option<IndexedPoi>> {
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
                    Address::Street(ref s) => {
                        (s.administrative_regions.clone(), s.country_codes.clone())
                    }
                    Address::Addr(ref s) => (
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
                Some(Address::Street(ref s)) => s.zip_codes.clone(),
                Some(Address::Addr(ref a)) => a.zip_codes.clone(),
                None => vec![],
            };

            res.poi.administrative_regions = admins;

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

fn properties_from_tags(tags: HashMap<String, Option<String>>) -> BTreeMap<String, String> {
    tags.into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect()
}

fn build_poi_type_text(
    class: &str,
    subclass: &str,
    tags: &HashMap<String, Option<String>>,
) -> String {
    /*
        To index certain tags (in addition to class and subclass), we use
        the field "poi_type.name" in a convoluted way.
        In the POI mapping configuration defined in mimirsbrunn, this field in indexed
        using a "word" analyzer.

        So each key/value must be defined as a word in this field, using the following format:
            * "class_<class_name>"
            * "subclass_<subclass_name>"
            * "<tag_key>:<tag_value>" (e.g "cuisine:japanese")

        When the tag contains multiple values (separated by ";"), these values are split
        and indexed as distinct tag values.
    */
    std::array::IntoIter::new([format!("class_{}", class), format!("subclass_{}", subclass)])
        .chain(
            TAGS_TO_INDEX_AS_POI_TYPE_NAME
                .iter()
                .map(|tag| {
                    let values = tags.get(*tag).unwrap_or(&None).clone().unwrap_or_default();
                    if values.is_empty() {
                        return vec![];
                    }
                    values
                        .split(';')
                        .map(|v| format!("{}:{}", tag, v))
                        .collect::<Vec<_>>()
                })
                .flatten(),
        )
        .join(" ")
}

fn build_poi_properties(
    row: &tokio_postgres::Row,
    mut properties: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    if let Ok(poi_subclass) = row.try_get("subclass") {
        properties.insert("poi_subclass".to_string(), poi_subclass);
    };

    if let Ok(poi_class) = row.try_get("class") {
        properties.insert("poi_class".to_string(), poi_class);
    };

    properties
}

fn build_names(langs: &[String], properties: &BTreeMap<String, String>) -> I18nProperties {
    const NAME_TAG_PREFIX: &str = "name:";

    let properties = properties
        .iter()
        .filter_map(|(key, val)| {
            if key.starts_with(&NAME_TAG_PREFIX) {
                let lang = key[NAME_TAG_PREFIX.len()..].to_string();
                if langs.contains(&lang) {
                    return Some(Property {
                        key: lang,
                        value: val.to_string(),
                    });
                }
            }
            None
        })
        .collect();

    I18nProperties(properties)
}
