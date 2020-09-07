use super::mimir;
use super::DATASET;
use super::{ElasticSearchWrapper, PostgresWrapper};
use geo_types as geo;
use mimirsbrunn::utils;
use std;
use std::f64;
use std::sync::Arc;

// Init the Postgres Wrapper
fn init_tests(
    es_wrapper: &mut ElasticSearchWrapper,
    pg_wrapper: &PostgresWrapper,
    country_code: &str,
) {
    let mut conn = pg_wrapper.get_conn();

    conn.batch_execute(include_str!("data/tables.sql"))
        .expect("failed to initialize tables");

    conn.batch_execute(include_str!("data/data.sql"))
        .expect("failed to populate tables");

    conn.batch_execute(include_str!("data/functions.sql"))
        .expect("failed to define SQL functions");

    load_es_data(es_wrapper, country_code);
}

fn load_es_data(es_wrapper: &mut ElasticSearchWrapper, country_code: &str) {
    let city = make_test_admin("bob's town", country_code);
    let test_address = make_test_address(city.clone());
    let addresses = std::iter::once(test_address);
    es_wrapper.index(DATASET, addresses);
    let cities = std::iter::once(city);

    es_wrapper.index(DATASET, cities);
}

fn make_test_admin(name: &str, country_code: &str) -> mimir::Admin {
    let p = |x, y| geo::Coordinate { x, y };

    let boundary = geo::MultiPolygon(vec![geo::Polygon::new(
        geo::LineString(vec![
            p(0., 0.),
            p(20., 0.),
            p(20., 20.),
            p(0., 20.),
            p(0., 0.),
        ]),
        vec![],
    )]);
    mimir::Admin {
        id: name.to_string(),
        level: 8,
        name: name.to_string(),
        label: name.to_string(),
        zip_codes: vec!["421337".to_string()],
        weight: 0f64,
        coord: ::mimir::Coord::new(4.0, 4.0),
        boundary: Some(boundary),
        insee: "outlook".to_string(),
        zone_type: Some(cosmogony::ZoneType::City),
        labels: mimir::I18nProperties::default(),
        names: mimir::I18nProperties::default(),
        codes: vec![mimir::Code {
            name: "ISO3166-1:alpha2".to_string(),
            value: country_code.to_string(),
        }],
        ..Default::default()
    }
}

fn make_test_address(city: mimir::Admin) -> mimir::Addr {
    let country_codes = utils::find_country_codes(std::iter::once(&city));

    let street = mimir::Street {
        id: "1234".to_string(),
        name: "test".to_string(),
        label: "test (bob's town)".to_string(),
        administrative_regions: vec![Arc::new(city)],
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        coord: mimir::Coord::new(1., 1.),
        country_codes: country_codes.clone(),
        ..Default::default()
    };
    mimir::Addr {
        id: format!("addr:{};{}", 1., 1.),
        house_number: "1234".to_string(),
        name: "1234 test".to_string(),
        street,
        label: "1234 test (bob's town)".to_string(),
        coord: mimir::Coord::new(1., 1.),
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        distance: None,
        approx_coord: None,
        country_codes,
        context: None,
    }
}

fn get_label(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(ref s) => &s.label,
        &mimir::Address::Addr(ref a) => &a.label,
    }
}

fn get_name(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(ref s) => &s.name,
        &mimir::Address::Addr(ref a) => &a.name,
    }
}

fn get_house_number(address: &mimir::Address) -> &str {
    match address {
        &mimir::Address::Street(_) => &"",
        &mimir::Address::Addr(ref a) => &a.house_number,
    }
}

fn get_coord(address: &mimir::Address) -> &mimir::Coord {
    match address {
        &mimir::Address::Street(ref s) => &s.coord,
        &mimir::Address::Addr(ref a) => &a.coord,
    }
}

fn get_zip_codes(address: &mimir::Address) -> Vec<String> {
    match address {
        &mimir::Address::Street(ref s) => s.zip_codes.clone(),
        &mimir::Address::Addr(ref a) => a.zip_codes.clone(),
    }
}

pub fn main_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    let rows = &pg_wrapper.get_rows(&"osm_poi_point");
    assert_eq!(rows.len(), 7);
    let rows = &pg_wrapper.get_rows(&"osm_poi_polygon");
    assert_eq!(rows.len(), 3);

    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        9 // 5 valid points + 2 valid polygons + 1 airport + 1 hamlet
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    let pois: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
        .collect();
    assert_eq!(&pois.len(), &1);

    // Test that the place "Ocean Studio" is a POI
    let ocean_place = &pois[0];
    assert!(&ocean_place.is_poi());

    // Test that the coord property of a POI has been well loaded
    // We test latitude and longitude
    let ocean_poi = &ocean_place.poi().unwrap();
    assert_eq!(&ocean_poi.id, "osm:node:5589618289");
    let coord_ocean_poi = &ocean_poi.coord;
    assert_relative_eq!(coord_ocean_poi.lat(), 1., epsilon = f64::EPSILON);
    assert_relative_eq!(coord_ocean_poi.lon(), 1., epsilon = f64::EPSILON);

    // Test Label
    let label_ocean_poi = &ocean_poi.label;
    assert_eq!(label_ocean_poi, &"Ocean Studio (bob's town)");
    // Test poi_type
    let poi_type_ocean_poi = &ocean_poi.poi_type.name;
    assert_eq!(poi_type_ocean_poi, &"class_cafe subclass_cafe");

    // Test Properties: the amenity property for this POI should be "cafe"
    let properties_ocean_poi = &ocean_poi.properties;
    let amenity_tag = properties_ocean_poi
        .into_iter()
        .find(|&p| p.key == "amenity")
        .unwrap();
    assert_eq!(amenity_tag.value, "cafe".to_string());

    // Test Address: we get the address from elasticsearch associated to a POI and we check that
    // its associated information are correct.
    // To guarantee the rubber found an address we have put a fake address close to the location of
    // the POI in the init() method.
    let address_ocean_poi = ocean_poi.address.as_ref().unwrap();
    let address_label = get_label(&address_ocean_poi);
    assert_eq!(address_label, &"1234 test (bob's town)".to_string());
    let address_house_number = get_house_number(&address_ocean_poi);
    assert_eq!(address_house_number, "1234".to_string());
    let address_coord = get_coord(&address_ocean_poi);
    assert_eq!(address_coord.lat(), 1.);
    assert_eq!(address_coord.lon(), 1.);
    let zip_code = get_zip_codes(&address_ocean_poi);
    assert_eq!(zip_code, vec!["12345".to_string()]);

    let le_nomade_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Le nomade", |_| true)
        .collect();
    assert_eq!(&le_nomade_query.len(), &1);
    let le_nomade = &le_nomade_query[0];
    assert!(&le_nomade.is_poi());
    let le_nomade = &le_nomade.poi().unwrap();
    assert_eq!(&le_nomade.id, "osm:way:42"); // the id in the database is '-42', so it's a way
                                             // this poi has addresses osm tags, we should have read it
    let le_nomade_addr = le_nomade.address.as_ref().unwrap();
    assert_eq!(get_label(le_nomade_addr), "7 rue spontini (bob's town)");
    assert_eq!(get_name(le_nomade_addr), "7 rue spontini");
    assert_eq!(get_house_number(le_nomade_addr), &"7".to_string());
    assert_eq!(get_zip_codes(le_nomade_addr), vec!["75016".to_string()]);

    // Test that the airport 'Isla Cristina Agricultural Airstrip' has been imported in the elastic wrapper
    let airport_cristina: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Isla Cristina", |_| true)
        .collect();
    assert_eq!(&airport_cristina.len(), &1);
    assert!(&airport_cristina[0].is_poi());

    // Test the airport id
    let airport = &airport_cristina[0].poi().unwrap();
    assert_eq!(&airport.id, "osm:node:4505823836");

    // Test the airport coord
    let airport_coord = &airport.coord;
    assert_relative_eq!(airport_coord.lat(), 5.0, epsilon = f64::EPSILON);
    assert_relative_eq!(airport_coord.lon(), 5.0, epsilon = f64::EPSILON);

    // Test the airport poi_class and poi_subclass
    let properties_airport = &airport.properties;
    let poi_class = properties_airport
        .into_iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.value, "aerodrome".to_string());
    let poi_subclass = properties_airport
        .into_iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.value, "airport".to_string());

    // the '4 gusto' has a tag addr:street but no housenumber, we therefore get an address without
    // a housenumber.
    let gusto_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:4 gusto", |_| true)
        .collect();
    assert_eq!(&gusto_query.len(), &1);
    let gusto = &gusto_query[0];
    assert!(&gusto.is_poi());
    let gusto = &gusto.poi().unwrap();
    assert_eq!(&gusto.id, "osm:node:5590601521");
    let bob = gusto.address.as_ref().unwrap();
    assert_eq!(get_label(bob), "rue spontini (bob's town)");
    assert_eq!(get_house_number(bob), "");

    // the Spagnolo has some osm address tags and no addr:postcode
    // we should still read it's address from osm
    let spagnolo_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .collect();
    assert_eq!(&spagnolo_query.len(), &1);
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = spagnolo.address.as_ref().unwrap();
    assert_eq!(get_label(spagnolo_addr), "12 rue bob (bob's town)");
    assert_eq!(get_house_number(spagnolo_addr), &"12".to_string());
    assert!(get_zip_codes(spagnolo_addr).is_empty());

    // Test that two "Tour Eiffel" POI should have been imported: the hotel + the monument
    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(Tour Eiffel)", |_| true)
        .collect();
    assert_eq!(&eiffels.len(), &2);

    // Test they are both POI
    assert!(&eiffels.iter().all(|ref p| p.is_poi()));

    // Test their weight are not both equal to 0.0
    assert!(!&eiffels
        .iter()
        .map(|ref mut p| p.poi().unwrap())
        .all(|p| p.weight == 0.0f64));

    let hamlet_somewhere: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:(I am a lost sheep)", |_| true)
        .collect();
    assert_eq!(&hamlet_somewhere.len(), &1);
    assert!(&hamlet_somewhere[0].is_poi());
    let hamlet_somewhere = &hamlet_somewhere[0].poi().unwrap();
    let properties_hamlet_somewhere = &hamlet_somewhere.properties;
    let poi_class = properties_hamlet_somewhere
        .into_iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    assert_eq!(poi_class.value, "locality".to_string());
    let poi_subclass = properties_hamlet_somewhere
        .into_iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(poi_subclass.value, "hamlet".to_string());

    // Test class/subclass for place_of_worship
    let church_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:saint-ambroise", |_| true)
        .collect();
    assert_eq!(&church_query.len(), &1);
    let church = &church_query[0].poi().unwrap();
    assert_eq!(church.name, "Église Saint-Ambroise");
    let church_class = church
        .properties
        .iter()
        .find(|&p| p.key == "poi_class")
        .unwrap();
    let church_subclass = church
        .properties
        .iter()
        .find(|&p| p.key == "poi_subclass")
        .unwrap();
    assert_eq!(church_class.value, "place_of_worship");
    assert_eq!(church_subclass.value, "christian");

    // 2 pois in nosearch index
    let nosearch_pois: Vec<mimir::Poi> = es_wrapper
        .rubber
        .get_all_objects_from_index("munin_poi_nosearch")
        .expect("failed to fetch poi_nosearch documents");
    assert_eq!(nosearch_pois.len(), 2);
}

pub fn bbox_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            format!("--bounding-box=0, 0, 3.5, 3.5"),
        ],
        &es_wrapper,
    );

    // We filtered the import by a bounding box, we still have 6 rows in PG
    let rows = &pg_wrapper.get_rows(&"osm_poi_point");
    assert_eq!(rows.len(), 7);
    // but there is only 3 elements in the ES now, 'Le nomade' and 'Isla Cristina Agricultural Airstrip'
    // have been filtered
    assert_eq!(
        es_wrapper
            .search_and_filter("name:*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        5
    );
}

pub fn test_with_langs(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            "--lang=ru".into(),
            "--lang=it".into(),
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    // with the fields "labels" and "names"
    let pois: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:Ocean*", |_| true)
        .collect();
    let ocean_poi = &pois[0].poi().unwrap();
    assert!(ocean_poi
        .names
        .0
        .iter()
        .any(|p| p.key == "ru" && p.value == "студия океана"));

    assert!(ocean_poi
        .names
        .0
        .iter()
        .any(|p| p.key == "it" && p.value == "Oceano Studioso"));

    assert!(ocean_poi
        .labels
        .0
        .iter()
        .any(|p| p.key == "ru" && p.value == "студия океана (bob\'s town)"));

    assert!(ocean_poi
        .labels
        .0
        .iter()
        .any(|p| p.key == "it" && p.value == "Oceano Studioso (bob\'s town)"));
}

pub fn test_address_format(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    // Import data with DE as country code in admins
    init_tests(&mut es_wrapper, &pg_wrapper, "DE");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
        ],
        &es_wrapper,
    );

    let spagnolo_query: Vec<mimir::Place> = es_wrapper
        .search_and_filter("name:spagnolo", |_| true)
        .collect();
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = spagnolo.address.as_ref().unwrap();

    // German format: housenumber comes after street name
    assert_eq!(get_label(spagnolo_addr), "rue bob 12 (bob's town)");
    assert_eq!(get_house_number(spagnolo_addr), &"12".to_string());
}

pub fn test_current_country_label(
    mut es_wrapper: ElasticSearchWrapper,
    pg_wrapper: PostgresWrapper,
) {
    init_tests(&mut es_wrapper, &pg_wrapper, "FR");
    let fafnir = concat!(env!("OUT_DIR"), "/../../../fafnir");
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            "--lang=en".into(),
        ],
        &es_wrapper,
    );

    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("names.en:(Eiffel Tower)", |_| true)
        .collect();

    assert_eq!(eiffels.len(), 1);
    let eiffel_tower = eiffels[0].poi().unwrap();

    assert!(!eiffel_tower.labels.0.iter().any(|l| l.key == "fr"));

    // Now check that we have the fr label too!
    super::launch_and_assert(
        fafnir,
        vec![
            format!("--dataset={}", DATASET),
            format!("--es={}", &es_wrapper.host()),
            format!("--pg=postgres://test@{}/test", &pg_wrapper.host()),
            "--lang=fr".into(),
            "--lang=en".into(),
        ],
        &es_wrapper,
    );
    let eiffels: Vec<mimir::Place> = es_wrapper
        .search_and_filter("names.en:(Eiffel Tower)", |_| true)
        .collect();
    assert_eq!(eiffels.len(), 1);
    let eiffel_tower = eiffels[0].poi().unwrap();
    assert!(eiffel_tower
        .labels
        .0
        .iter()
        .any(|l| l.key == "fr" && l.value == "Tour Eiffel (bob's town)"));
    assert!(eiffel_tower
        .names
        .0
        .iter()
        .any(|n| n.key == "fr" && n.value == "Tour Eiffel"))
}
