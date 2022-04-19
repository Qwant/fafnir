//! Helpers to query the list of searchable POIs from a postgres database previously populated with
//! imposm.

pub fn fetch_all_pois_query(bbox: Option<[f64; 4]>) -> PoisQuery {
    let mut query = PoisQuery::new()
        .with_table(TableQuery::new("all_pois(14)").id_column("global_id"))
        .with_table(
            TableQuery::new("osm_aerodrome_label_point")
                .override_class("'aerodrome'")
                .override_subclass("'airport'"),
        )
        .with_table(
            TableQuery::new("osm_city_point")
                .override_class("'locality'")
                .override_subclass("'hamlet'")
                .filter("name <> '' AND place='hamlet'"),
        )
        .with_table(
            TableQuery::new("osm_water_lakeline")
                .override_class("'water'")
                .override_subclass("'lake'"),
        )
        .with_table(
            TableQuery::new("osm_water_point")
                .override_class("'water'")
                .override_subclass("'water'"),
        )
        .with_table(
            TableQuery::new("osm_marine_point")
                .override_class("'water'")
                .override_subclass("place"),
        );

    if let Some(bbox) = bbox {
        query = query.bbox(bbox);
    }

    query
}

#[derive(Default)]
pub struct PoisQuery {
    bbox: Option<[f64; 4]>,
    tables: Vec<TableQuery>,
}

impl PoisQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bbox(mut self, bbox: [f64; 4]) -> Self {
        self.bbox = Some(bbox);
        self
    }

    pub fn with_table(mut self, table_query: TableQuery) -> Self {
        self.tables.push(table_query);
        self
    }

    pub fn build(&self) -> String {
        let mut result = format!(
            "
                SELECT
                    id,
                    lon,
                    lat,
                    class,
                    name,
                    tags,
                    subclass,
                    mapping_key,
                    poi_display_weight(name, subclass, mapping_key, tags)::float as weight
                FROM
                    ({}) AS unionall
            ",
            self.tables
                .iter()
                .map(TableQuery::build)
                .collect::<Vec<_>>()
                .join(" UNION ALL ")
        );

        if let Some([lat1, lon1, lat2, lon2]) = self.bbox {
            result.push_str(&format!(
                "WHERE ST_MakeEnvelope({}, {}, {}, {}, 4326) && st_transform(geometry, 4326)",
                lat1, lon1, lat2, lon2
            ));
        }

        result
    }
}

pub struct TableQuery {
    table: String,
    id_column: String,
    filter: Option<String>,
    override_class: Option<String>,
    override_subclass: Option<String>,
}

impl TableQuery {
    pub fn new<S: Into<String>>(table: S) -> Self {
        Self {
            table: table.into(),
            id_column: "global_id_from_imposm(osm_id)".to_string(),
            filter: None,
            override_class: None,
            override_subclass: None,
        }
    }

    pub fn id_column<S: Into<String>>(mut self, id_column: S) -> Self {
        self.id_column = id_column.into();
        self
    }

    pub fn filter<S: Into<String>>(mut self, filter: S) -> Self {
        self.filter = Some(filter.into());
        self
    }

    pub fn override_class<S: Into<String>>(mut self, class: S) -> Self {
        self.override_class = Some(class.into());
        self
    }

    pub fn override_subclass<S: Into<String>>(mut self, subclass: S) -> Self {
        self.override_subclass = Some(subclass.into());
        self
    }

    pub fn build(&self) -> String {
        let mut result = format!(
            "
                SELECT
                    geometry,
                    {id_column} AS id,
                    ST_X({geometry_point}) AS lon,
                    ST_Y({geometry_point}) AS lat,
                    name,
                    tags,
                    {class},
                    {mapping_key},
                    {subclass}
                FROM {table}
            ",
            table = self.table,
            id_column = self.id_column,
            class = self
                .override_class
                .as_ref()
                .map_or_else(|| "class".to_string(), |name| format!("{name} AS class")),
            mapping_key = self.override_class.as_ref().map_or_else(
                || "mapping_key".to_string(),
                |name| format!("{name} AS mapping_key")
            ),
            subclass = self.override_subclass.as_ref().map_or_else(
                || "subclass".to_string(),
                |name| format!("{name} AS subclass")
            ),
            geometry_point = "ST_Transform(ST_PointOnSurface(geometry), 4326)",
        );

        if let Some(ref filter) = self.filter {
            result.push_str(&format!(" WHERE {filter}"));
        }

        result
    }
}
