use crate::addresses::find_address;
use crate::addresses::iter_admins;
use lazy_static::lazy_static;
use mimir::rubber::Rubber;
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

lazy_static! {
    static ref NON_SEARCHABLE_ITEMS: BTreeSet<(String, String)> = [
        /* List of (mapping_key, subclass) */
        ("highway", "bus_stop"),
        ("barrier", "gate"),
        ("amenity", "waste_basket"),
        ("amenity", "post_box"),
        ("tourism", "information"),
        ("amenity", "recycling"),
        ("barrier", "lift_gate"),
        ("barrier", "bollard"),
        ("barrier", "cycle_barrier"),
        ("amenity", "bicycle_rental"),
        ("tourism", "artwork"),
        ("amenity", "toilets"),
        ("leisure", "playground"),
        ("amenity", "telephone"),
        ("amenity", "taxi"),
        ("leisure", "pitch"),
        ("amenity", "shelter"),
        ("barrier", "sally_port"),
        ("barrier", "stile"),
        ("amenity", "ferry_terminal"),
        ("amenity", "post_office"),
        ("railway", "subway_entrance"),
        ("railway", "train_station_entrance"),
    ]
    .iter()
    .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
    .collect();
}

pub struct IndexedPoi {
    pub poi: Poi,
    pub is_searchable: bool,
}

impl IndexedPoi {
    pub fn from_row(row: Row, langs: &[String]) -> Option<IndexedPoi> {
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

        let names = build_names(langs, &row_properties)
            .unwrap_or_else(|_| mimir::I18nProperties::default());

        let properties = build_poi_properties(&row, &id, row_properties).unwrap_or_else(|_| vec![]);

        let is_searchable =
            !name.is_empty() && !NON_SEARCHABLE_ITEMS.contains(&(mapping_key, subclass));

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

        Some(IndexedPoi { poi, is_searchable })
    }

    pub fn locate_poi(
        mut self,
        geofinder: &AdminGeoFinder,
        rubber: &mut Rubber,
        langs: &[String],
    ) -> Option<IndexedPoi> {
        let poi_address = find_address(&self.poi, geofinder, rubber);

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
                let admins = geofinder.get(&self.poi.coord);
                let country_codes = find_country_codes(iter_admins(&admins));
                (admins, country_codes)
            });

        if admins.is_empty() {
            debug!("The poi {} is not on any admins", &self.poi.id);
            return None;
        }

        let zip_codes = match poi_address {
            Some(mimir::Address::Street(ref s)) => s.zip_codes.clone(),
            Some(mimir::Address::Addr(ref a)) => a.zip_codes.clone(),
            _ => vec![],
        };

        self.poi.administrative_regions = admins;
        self.poi.address = poi_address;
        self.poi.label = format_poi_label(
            &self.poi.name,
            iter_admins(&self.poi.administrative_regions),
            &country_codes,
        );
        self.poi.labels = format_international_poi_label(
            &self.poi.names,
            &self.poi.name,
            &self.poi.label,
            iter_admins(&self.poi.administrative_regions),
            &country_codes,
            langs,
        );
        self.poi.zip_codes = zip_codes;
        Some(self)
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

fn build_names(langs: &[String], properties: &[Property]) -> Result<mimir::I18nProperties, String> {
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

    Ok(mimir::I18nProperties(properties))
}
