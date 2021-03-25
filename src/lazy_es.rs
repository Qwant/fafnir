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
// --- LazyEs
// ---

pub enum LazyEs<'p, T> {
    Value(T),
    NeedEsQuery {
        header: serde_json::Value,
        query: serde_json::Value,
        progress: Box<dyn FnOnce(&str) -> LazyEs<'p, T> + 'p>,
    },
}

impl<'p, T: 'p> LazyEs<'p, T> {
    pub fn get(self) -> Option<T> {
        match self {
            Self::Value(x) => Some(x),
            _ => None,
        }
    }

    pub fn has_value(&self) -> bool {
        matches!(self, Self::Value(_))
    }

    pub fn map<U>(self, func: impl FnOnce(T) -> U + 'p) -> LazyEs<'p, U> {
        self.then(move |x| LazyEs::Value(func(x)))
    }

    pub fn then<U>(self, func: impl FnOnce(T) -> LazyEs<'p, U> + 'p) -> LazyEs<'p, U> {
        match self {
            Self::Value(x) => func(x),
            Self::NeedEsQuery {
                header,
                query,
                progress,
            } => LazyEs::NeedEsQuery {
                header,
                query,
                progress: Box::new(move |val| progress(val).then(func)),
            },
        }
    }
}

pub fn batch_make_progress<'a, T: 'a>(rubber: &mut Rubber, partials: &mut [LazyEs<'a, T>]) {
    let need_progress: Vec<_> = partials
        .iter_mut()
        .filter(|partial| !partial.has_value())
        .collect();

    let body: String = {
        need_progress
            .iter()
            .filter_map(|partial| match partial {
                LazyEs::NeedEsQuery { header, query, .. } => {
                    Some(format!("{}\n{}\n", header.to_string(), query.to_string()))
                }
                _ => None,
            })
            .collect()
    };

    let es_response = rubber
        .post("_msearch", &body)
        .expect("ES query failed")
        .text()
        .expect("failed to read ES response");

    let responses = parse_es_multi_response(&es_response).expect("failed to parse ES responses");
    assert_eq!(responses.len(), need_progress.len());

    for (partial, res) in need_progress.into_iter().zip(responses) {
        match partial {
            LazyEs::NeedEsQuery { progress, .. } => {
                let progress = std::mem::replace(progress, Box::new(|_| unreachable!()));
                *partial = progress(res);
            }
            LazyEs::Value(_) => unreachable!("values expected to be filtered out"),
        }
    }
}

pub fn batch_make_progress_until_value<'a, T: 'a>(
    rubber: &mut Rubber,
    mut partials: Vec<LazyEs<'a, T>>,
) -> Vec<T> {
    while partials.iter().any(|x| !x.has_value()) {
        batch_make_progress(rubber, partials.as_mut_slice());
    }

    partials
        .into_iter()
        .map(|partial| partial.get().unwrap())
        .collect()
}
