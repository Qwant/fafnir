extern crate postgres;
extern crate mimir;
extern crate mimirsbrunn;
#[macro_use]
extern crate log;
extern crate geo;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use std::collections::HashMap;
use postgres::{Connection, TlsMode};
use postgres::rows::Row;
use mimir::MimirObject;
use mimir::{Poi, Coord, Admin, PoiType, Property};
use mimir::rubber::Rubber;
use mimirsbrunn::admin_geofinder::AdminGeoFinder;


fn index_pois<T>(mut rubber: Rubber, dataset: &str, pois: T)
	where T: Iterator<Item = Poi> {
	let poi_index = rubber.make_index(Poi::doc_type(), dataset).unwrap();

	match rubber.bulk_index(&poi_index, pois) {
		Err(e) => panic!("Failed to bulk insert pois because: {}", e),
		Ok(nb) => info!("Nb of indexed pois: {}", nb)
	}

	rubber.publish_index(Poi::doc_type(), dataset, poi_index, Poi::is_geo_data())
		  .unwrap();
}

fn build_poi_id(row: &Row) -> String {
	format!("pg:{source}:{id}", 
		source=row.get::<_, String>("source"),
		id=row.get::<_, i64>("osm_id")
	)
}

fn build_poi_properties(row: &Row) -> Vec<Property> {
	row.get::<_, HashMap<_,_>>("tags").into_iter()
		.map(|(k, v)| Property{
			key: k,
			value: v.unwrap_or("".to_string()) 
		})
		.collect()
}

fn build_poi(row: Row, geofinder: &AdminGeoFinder) -> Poi {
	let name: String = row.get("name");
	let class: String = row.get("class");
	let lat = row.get("lat");
	let lon = row.get("lon");
	let admins = geofinder.get(&geo::Coordinate {
		x: lat,
		y: lon
	});

	Poi {
		id: build_poi_id(&row),
		coord: Coord::new(lat, lon),
		administrative_regions: admins,
		poi_type: PoiType {
			id: class.clone(),
			name: class
		},
		label: name.clone(),
		name: name,
		weight: 0.,
		zip_codes: vec![],
		properties: build_poi_properties(&row)
	}
}

fn load_and_index_pois(){
	mimir::logger_init();

    let conn = Connection::connect(
    	"postgres://gis@localhost:12345", TlsMode::None
    	).unwrap();

   	let mut rubber = Rubber::new("http://localhost:9200");
   	let dataset = "dataset-id";

   	let admins = rubber
   	        .get_admins_from_dataset(dataset)
   	        .unwrap_or_else(|err| {
   	            warn!(
   	                "Administratives regions not found in es db for dataset {}. (error: {})",
   	                dataset, err
   	            );
   	            vec![]
   	        });
   	let admins_geofinder = admins.into_iter().collect();

    let rows = &conn.query(
    	"SELECT osm_id,
		    st_x(st_transform(geometry, 4326)) as lon,
		    st_y(st_transform(geometry, 4326)) as lat,
		    poi_class(subclass, mapping_key) AS class,
		    name,
		    tags,
		    'osm_poi_point' as source
		    FROM osm_poi_point WHERE name <> ''
	    UNION ALL
	    	SELECT osm_id,
		    st_x(st_transform(geometry, 4326)) as lon,
		    st_y(st_transform(geometry, 4326)) as lat,
		    poi_class(subclass, mapping_key) AS class,
		    name,
		    tags,
		    'osm_poi_polygon' as source
		    FROM osm_poi_polygon WHERE name <> ''
		LIMIT 30", 
	    &[]).unwrap();
  	

  	let pois = rows.iter().map(|r| build_poi(r, &admins_geofinder));
  	index_pois(rubber, dataset, pois);
 	
  	// println!("{:?}", pois.collect::<Vec<_>>());
}


#[derive(StructOpt, Debug)]
struct Args {
    /// openaddresses files. Can be either a directory or a file.
    #[structopt(long = "es")]
    es: String,
    /// Elasticsearch parameters.
    #[structopt(long = "connection-string",
                default_value = "http://localhost:9200/")]
    connection_string: String,
    /// Name of the dataset.
    #[structopt(short = "d", long = "dataset")]
    dataset: String,
}

fn main() {
	load_and_index_pois()
}

