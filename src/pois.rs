use lazy_static::lazy_static;
use mimir::Poi;
use std::collections::BTreeSet;

lazy_static! {
    static ref NOT_SEARCHABLE_ITEMS: BTreeSet<(String, String)> = [
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
    .into_iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect();
}

pub struct IndexedPoi {
    pub poi: Poi,
    pub searchable: bool,
}

pub fn is_searchable(poi: &Poi, mapping_key: &str, subclass: &str) -> bool {
    return !poi.name.is_empty()
        && !NOT_SEARCHABLE_ITEMS.contains(&(mapping_key.into(), subclass.into()));
}
