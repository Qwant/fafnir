use mimir::rubber::Rubber;
use mimir::Poi;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;
use mimirsbrunn::labels::format_addr_name_and_label;
use mimirsbrunn::labels::format_street_label;
use mimirsbrunn::utils::find_country_codes;
use std::ops::Deref;
use std::sync::Arc;

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
        .map(|p| p.value.to_owned());
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
        approx_coord: poi.approx_coord.clone(),
        weight,
        zip_codes: postcodes,
        distance: None,
        country_codes,
        context: None,
    })
}

/// Build mimir Address from Poi,using osm address tags (if present)
/// or using reverse geocoding.
///
/// We also search for the admins that contains the coordinates of the poi
/// and add them as the address's admins.
pub fn find_address(
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

pub fn iter_admins(admins: &[Arc<mimir::Admin>]) -> impl Iterator<Item = &mimir::Admin> + Clone {
    admins.iter().map(|a| a.deref())
}
