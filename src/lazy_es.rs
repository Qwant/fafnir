use mimir::rubber::Rubber;
use serde::Deserialize;
use serde_json::value::RawValue;

#[derive(Deserialize)]
pub struct EsHit<U> {
    #[serde(rename = "_source")]
    pub source: U,
    #[serde(rename = "_type")]
    pub doc_type: String,
}

#[derive(Deserialize)]
pub struct EsHits<U> {
    pub hits: Vec<EsHit<U>>,
}

#[derive(Deserialize)]
pub struct EsResponse<U> {
    pub hits: EsHits<U>,
}

pub fn parse_es_multi_response(es_multi_response: &str) -> Result<Vec<&str>, String> {
    #[derive(Deserialize)]
    struct EsResponse<'a> {
        #[serde(borrow)]
        responses: Vec<&'a RawValue>,
    }

    let es_response: EsResponse = serde_json::from_str(es_multi_response)
        .map_err(|err| format!("failed to parse ES multi response: {:?}", err))?;

    Ok(es_response
        .responses
        .into_iter()
        .map(RawValue::get)
        .collect())
}

// ---
// --- PartialResult
// ---

pub enum PartialResult<'p, T> {
    Value(T),
    NeedEsQuery {
        header: serde_json::Value,
        query: serde_json::Value,
        progress: Box<dyn FnOnce(&str) -> PartialResult<'p, T> + 'p>,
    },
}

impl<'p, T: 'p> PartialResult<'p, T> {
    pub fn map<U>(self, func: impl FnOnce(T) -> U + 'p) -> PartialResult<'p, U> {
        self.partial_map(move |x| PartialResult::Value(func(x)))
    }

    pub fn partial_map<U>(
        self,
        func: impl FnOnce(T) -> PartialResult<'p, U> + 'p,
    ) -> PartialResult<'p, U> {
        match self {
            Self::Value(x) => func(x),
            Self::NeedEsQuery {
                header,
                query,
                progress,
            } => PartialResult::NeedEsQuery {
                header,
                query,
                progress: Box::new(move |val| progress(val).partial_map(func)),
            },
        }
    }

    pub fn make_progress(self, rubber: &mut Rubber) -> PartialResult<'p, T> {
        match self {
            PartialResult::Value(_) => self,
            PartialResult::NeedEsQuery {
                header,
                query,
                progress,
            } => {
                let res = rubber
                    .post("_msearch", &format!("{}\n{}\n", header, query))
                    .expect("failed to reach ES")
                    .text()
                    .expect("failed to read ES multi response");

                let res_vec =
                    parse_es_multi_response(&res).expect("failed to parse ES multi response");

                assert_eq!(res_vec.len(), 1);
                progress(res_vec[0])
            }
        }
    }

    pub fn make_progress_until_value(self, rubber: &mut Rubber) -> T {
        match self {
            PartialResult::Value(x) => x,
            PartialResult::NeedEsQuery { .. } => {
                self.make_progress(rubber).make_progress_until_value(rubber)
            }
        }
    }
}
