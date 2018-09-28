extern crate cosmogony;

use super::mimir;
use super::DATASET;
use super::{ElasticSearchWrapper, PostgresWrapper};
use geo;
use postgres::Connection;
use std;
use std::f64;
use std::sync::Arc;

// Init the Postgres Wrapper
fn init_tests(es_wrapper: &mut ElasticSearchWrapper, pg_wrapper: &PostgresWrapper) {
    let conn = pg_wrapper.get_conn();
    create_tests_tables(&conn);
    populate_tables(&conn);
    load_poi_class_function(&conn);
    load_osm_id_function(&conn);
    load_es_data(es_wrapper);
}

fn create_tests_tables(conn: &Connection) {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_poi_point(
                         id                 serial primary key,
                         osm_id             bigint,
                         name               varchar,
                         name_en            varchar,
                         name_de            varchar,
                         tags               hstore,
                         subclass           varchar,
                         mapping_key        varchar,
                         station            varchar,
                         funicular          varchar,
                         information        varchar,
                         uic_ref            varchar,
                         geometry           geometry,
                         agg_stop           integer
                       )",
        &[],
    ).unwrap();
    conn.execute("TRUNCATE TABLE osm_poi_point", &[]).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_poi_polygon (
                         id                 serial primary key,
                         osm_id             bigint,
                         name               varchar,
                         name_en            varchar,
                         name_de            varchar,
                         tags               hstore,
                         subclass           varchar,
                         mapping_key        varchar,
                         station            varchar,
                         funicular          varchar,
                         information        varchar,
                         uic_ref            varchar,
                         geometry           geometry
                       )",
        &[],
    ).unwrap();
    conn.execute("TRUNCATE TABLE osm_poi_polygon", &[]).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS osm_aerodrome_label_point(
                         id                         serial primary key,
                         osm_id                     bigint,
                         name                       varchar,
                         name_en                    varchar,
                         name_de                    varchar,
                         tags                       hstore,
                         aerodrome_type             varchar,
                         aerodrome                  varchar,
                         military                   varchar,
                         iata                       varchar,
                         icao                       varchar,
                         ele                        varchar,
                         geometry                   geometry
                       )",
        &[],
    ).unwrap();
    conn.execute("TRUNCATE TABLE osm_aerodrome_label_point", &[])
        .unwrap();
}

fn populate_tables(conn: &Connection) {
    // this poi is located at lon=1, lat=1
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) 
    VALUES (5589618289, 'Ocean Studio',null,null, 'cafe', 'amenity',null,null,null,null, '0101000020E6100000000000000000F03F000000000000F03F'
    , '\"name\"=>\"Ocean Studio\", \"amenity\"=>\"cafe\", \"name_int\"=>\"Ocean Studio\", \"name:latin\"=>\"Ocean Studio\"')", &[]).unwrap();
    // this poi is located at lon=2, lat=2
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) 
    VALUES (5590210422, 'Spagnolo',null,null, 'clothes', 'shop',null,null,null,null, '0101000020E610000000000000000000400000000000000040'
    , '\"name\"=>\"Spagnolo\", \"shop\"=>\"clothes\", \"name_int\"=>\"Spagnolo\", \"name:latin\"=>\"Spagnolo\",\"addr:housenumber\"=>\"12\",\"addr:street\"=>\"rue bob\"')", &[]).unwrap();
    // this poi is located at lon=3, lat=3
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) 
    VALUES (5590601521, '4 gusto',null,null, 'cafe', 'amenity',null,null,null,null, '0101000020E610000000000000000008400000000000000840'
    , '\"name\"=>\"4 gusto\", \"amenity\"=>\"cafe\", \"name_int\"=>\"4 gusto\", \"name:latin\"=>\"4 gusto\",\"addr:street\"=>\"rue spontini\"')", &[]).unwrap();
    // this poi is located at lon=4, lat=4
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) 
    VALUES (-42, 'Le nomade',null,null, 'bar', 'amenity',null,null,null,null, '0101000020E610000000000000000010400000000000001040'
    , '\"name\"=>\"Le nomade\", \"amenity\"=>\"bar\", \"name:es\"=>\"Le nomade\", \"name_int\"=>\"Le nomade\", \"name:latin\"=>\"Le nomade\",\"addr:housenumber\"=>\"7\",\"addr:street\"=>\"rue spontini\",\"addr:postcode\"=>\"75016\"')", &[]).unwrap();
    // this poi is located at lon=5, lat=5
    conn.execute("INSERT INTO osm_aerodrome_label_point (id, osm_id, name, name_en, name_de, aerodrome_type, aerodrome, military, iata, icao, ele, geometry, tags) 
    VALUES (5934, 4505823836, 'Isla Cristina Agricultural Airstrip', null, null, null, null, null, null,  null, null, '0101000020E610000000000000000014400000000000001440'
    , '\"name\"=>\"Isla Cristina Agricultural Airstrip\", \"aeroway\"=>\"aerodrome\", \"name_int\"=>\"Isla Cristina Agricultural Airstrip\", \"name:latin\"=>\"Isla Cristina Agricultural Airstrip\"')", &[]).unwrap();

    // we also add a poi located at lon=-1, lat=-1, it won't be in an admin, so it must not be imported
    conn.execute("INSERT INTO osm_poi_point (osm_id, name, name_en, name_de, subclass, mapping_key, station, funicular, information, uic_ref, geometry, tags) 
    VALUES (12321, 'poi too far',null,null, 'bar', 'amenity',null,null,null,null, '0101000020E6100000000000000000F0BF000000000000F0BF'
    , '\"name\"=>\"poi too far\"')", &[]).unwrap();

    // aerodrom at the South Pole at lon=0, lat=-90 (Invalid coordinates in EPSG:4326)
    conn.execute("INSERT INTO osm_aerodrome_label_point (id, osm_id, name, name_en, name_de, aerodrome_type, aerodrome, military, iata, icao, ele, geometry, tags)
    VALUES (30334, 1042050310, 'South Pole Station Airport',null, null, null, null, null, null,  null, null, '0101000020110F0000714501E743E172BF010000000000F87F',
     '\"name\"=>\"South Pole Station Airport\", \"aeroway\"=>\"aerodrome\", \"name_int\"=>\"South Pole Station Airport\", \"name:latin\"=>\"South Pole Station Airport\"')", &[]).unwrap();
}

/// This function uses the poi_class function from
/// https://github.com/openmaptiles/openmaptiles/blob/master/layers/poi/class.sql
fn load_poi_class_function(conn: &Connection) {
    conn.execute("
            CREATE OR REPLACE FUNCTION poi_class(subclass TEXT, mapping_key TEXT)
            RETURNS TEXT AS $$
                SELECT CASE
                    WHEN subclass IN ('accessories','antiques','beauty','bed','boutique','camera','carpet','charity','chemist','chocolate','coffee','computer','confectionery','convenience','copyshop','cosmetics','garden_centre','doityourself','erotic','electronics','fabric','florist','frozen_food','furniture','video_games','video','general','gift','hardware','hearing_aids','hifi','ice_cream','interior_decoration','jewelry','kiosk','lamps','mall','massage','motorcycle','mobile_phone','newsagent','optician','outdoor','perfumery','perfume','pet','photo','second_hand','shoes','sports','stationery','tailor','tattoo','ticket','tobacco','toys','travel_agency','watches','weapons','wholesale') THEN 'shop'
                    WHEN subclass IN ('townhall','public_building','courthouse','community_centre') THEN 'town_hall'
                    WHEN subclass IN ('golf','golf_course','miniature_golf') THEN 'golf'
                    WHEN subclass IN ('fast_food','food_court') THEN 'fast_food'
                    WHEN subclass IN ('park','bbq') THEN 'park'
                    WHEN subclass IN ('bus_stop','bus_station') THEN 'bus'
                    WHEN (subclass='station' AND mapping_key = 'railway') OR subclass IN ('halt', 'tram_stop', 'subway') THEN 'railway'
                    WHEN (subclass='station' AND mapping_key = 'aerialway') THEN 'aerialway'
                    WHEN subclass IN ('subway_entrance','train_station_entrance') THEN 'entrance'
                    WHEN subclass IN ('camp_site','caravan_site') THEN 'campsite'
                    WHEN subclass IN ('laundry','dry_cleaning') THEN 'laundry'
                    WHEN subclass IN ('supermarket','deli','delicatessen','department_store','greengrocer','marketplace') THEN 'grocery'
                    WHEN subclass IN ('books','library') THEN 'library'
                    WHEN subclass IN ('university','college') THEN 'college'
                    WHEN subclass IN ('hotel','motel','bed_and_breakfast','guest_house','hostel','chalet','alpine_hut','camp_site') THEN 'lodging'
                    WHEN subclass IN ('chocolate','confectionery') THEN 'ice_cream'
                    WHEN subclass IN ('post_box','post_office') THEN 'post'
                    WHEN subclass IN ('cafe') THEN 'cafe'
                    WHEN subclass IN ('school','kindergarten') THEN 'school'
                    WHEN subclass IN ('alcohol','beverages','wine') THEN 'alcohol_shop'
                    WHEN subclass IN ('bar','nightclub') THEN 'bar'
                    WHEN subclass IN ('marina','dock') THEN 'harbor'
                    WHEN subclass IN ('car','car_repair','taxi') THEN 'car'
                    WHEN subclass IN ('hospital','nursing_home', 'clinic') THEN 'hospital'
                    WHEN subclass IN ('grave_yard','cemetery') THEN 'cemetery'
                    WHEN subclass IN ('attraction','viewpoint') THEN 'attraction'
                    WHEN subclass IN ('biergarten','pub') THEN 'beer'
                    WHEN subclass IN ('music','musical_instrument') THEN 'music'
                    WHEN subclass IN ('american_football','stadium','soccer','pitch') THEN 'stadium'
                    WHEN subclass IN ('art','artwork','gallery','arts_centre') THEN 'art_gallery'
                    WHEN subclass IN ('bag','clothes') THEN 'clothing_store'
                    WHEN subclass IN ('swimming_area','swimming') THEN 'swimming'
                    WHEN subclass IN ('castle','ruins') THEN 'castle'
                    ELSE subclass
                END;
            $$ LANGUAGE SQL IMMUTABLE;", &[]).unwrap();
}

/// This function uses the poi_class function from
/// https://github.com/QwantResearch/openmaptiles/blob/master/layers/poi/layer.sql#L11
fn load_osm_id_function(conn: &Connection) {
    conn.execute(
        "
        CREATE OR REPLACE FUNCTION global_id_from_imposm(imposm_id bigint)
            RETURNS TEXT AS $$
            SELECT CONCAT(
                'osm:',
                CASE WHEN imposm_id < -1e17 THEN CONCAT('relation:', -imposm_id-1e17)
                    WHEN imposm_id < 0 THEN CONCAT('way:', -imposm_id)
                    ELSE CONCAT('node:', imposm_id)
                END
            );
        $$ LANGUAGE SQL IMMUTABLE;
    ",
        &[],
    ).unwrap();
}

fn load_es_data(es_wrapper: &mut ElasticSearchWrapper) {
    let city = make_test_admin();
    let test_address = make_test_address(city.clone());
    let addresses = std::iter::once(test_address);
    es_wrapper.index(DATASET, addresses);
    let cities = std::iter::once(city);
    es_wrapper.index(DATASET, cities);
}

fn make_test_admin() -> mimir::Admin {
    let p = |x, y| geo::Point(geo::Coordinate { x: x, y: y });

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
        id: "bobs_town".to_string(),
        level: 8,
        name: "bob's town".to_string(),
        label: "bob's town".to_string(),
        zip_codes: vec!["421337".to_string()],
        weight: 0f64,
        coord: ::mimir::Coord::new(4.0, 4.0),
        boundary: Some(boundary),
        insee: "outlook".to_string(),
        zone_type: Some(cosmogony::ZoneType::City),
        bbox: None,
        parent_id: None
    }
}

fn make_test_address(city: mimir::Admin) -> mimir::Addr {
    let street = mimir::Street {
        id: "1234".to_string(),
        name: "test".to_string(),
        label: "test (bob's town)".to_string(),
        administrative_regions: vec![Arc::new(city)],
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
        coord: mimir::Coord::new(1., 1.),
    };
    mimir::Addr {
        id: format!("addr:{};{}", 1., 1.),
        house_number: "1234".to_string(),
        name: "1234 test".to_string(),
        street: street,
        label: "1234 test (bob's town)".to_string(),
        coord: mimir::Coord::new(1., 1.),
        weight: 50.0,
        zip_codes: vec!["12345".to_string()],
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
    init_tests(&mut es_wrapper, &pg_wrapper);
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

    // Test that the postgres wrapper contains 5 rows
    let rows = &pg_wrapper.get_rows();
    assert_eq!(rows.len(), 5);
    // but the elastic search contains only 4 because the poi "poi too far" has not been loaded
    assert_eq!(
        es_wrapper
            .search_and_filter("*.*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        5
    );

    // Test that the place "Ocean Studio" has been imported in the elastic wrapper
    let pois: Vec<mimir::Place> = es_wrapper
        .search_and_filter("Ocean Studio", |_| true)
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
        .search_and_filter("Le nomade", |_| true)
        .collect();
    assert_eq!(&le_nomade_query.len(), &1);
    let le_nomade = &le_nomade_query[0];
    assert!(&le_nomade.is_poi());
    let le_nomade = &le_nomade.poi().unwrap();
    assert_eq!(&le_nomade.id, "osm:way:42"); // the id in the database is '-42', so it's a way
                                             // this poi has addresses osm tags, we should have read it
    let le_nomade_addr = le_nomade.address.as_ref().unwrap();
    assert_eq!(
        get_label(le_nomade_addr),
        "7 rue spontini (bob's town)"
    );
    assert_eq!(
        get_name(le_nomade_addr),
        "7 rue spontini"
    );
    assert_eq!(get_house_number(le_nomade_addr), &"7".to_string());
    assert_eq!(get_zip_codes(le_nomade_addr), vec!["75016".to_string()]);

    // Test that the airport 'Isla Cristina Agricultural Airstrip' has been imported in the elastic wrapper
    let airport_cristina: Vec<mimir::Place> = es_wrapper
        .search_and_filter("Isla Cristina", |_| true)
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

    // the '4 gusto' has a tag addr:street but no housenumber, we should not read the address from osm
    // and since it's too far from another address it should not have an address
    let gusto_query: Vec<mimir::Place> =
        es_wrapper.search_and_filter("4 gusto", |_| true).collect();
    assert_eq!(&gusto_query.len(), &1);
    let gusto = &gusto_query[0];
    assert!(&gusto.is_poi());
    let gusto = &gusto.poi().unwrap();
    assert_eq!(&gusto.id, "osm:node:5590601521");
    assert!(&gusto.address.is_none());

    // the Spagnolo has some osm address tags and no addr:postcode
    // we should still read it's address from osm
    let spagnolo_query: Vec<mimir::Place> =
        es_wrapper.search_and_filter("spagnolo", |_| true).collect();
    assert_eq!(&spagnolo_query.len(), &1);
    let spagnolo = &spagnolo_query[0];
    assert!(&spagnolo.is_poi());
    let spagnolo = &spagnolo.poi().unwrap();
    assert_eq!(&spagnolo.id, "osm:node:5590210422");
    let spagnolo_addr = spagnolo.address.as_ref().unwrap();
    assert_eq!(
        get_label(spagnolo_addr),
        "12 rue bob (bob's town)"
    );
    assert_eq!(get_house_number(spagnolo_addr), &"12".to_string());
    assert!(get_zip_codes(spagnolo_addr).is_empty());
}

pub fn bbox_test(mut es_wrapper: ElasticSearchWrapper, pg_wrapper: PostgresWrapper) {
    init_tests(&mut es_wrapper, &pg_wrapper);
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

    // We filtered the import by a bounding box, we still have 5 rows in PG
    let rows = &pg_wrapper.get_rows();
    assert_eq!(rows.len(), 5);
    // but there is only 3 elements in the ES now, 'Le nomade' and 'Isla Cristina Agricultural Airstrip'
    // have been filtered
    assert_eq!(
        es_wrapper
            .search_and_filter("*.*", |p| p.is_poi())
            .collect::<Vec<_>>()
            .len(),
        3
    );
}
