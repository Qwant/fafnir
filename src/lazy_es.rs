use mimir::rubber::Rubber;
use serde::Deserialize;
use serde_json::value::RawValue;

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
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Value(x) => Some(x),
            _ => None,
        }
    }

    pub fn header_and_query(&self) -> Option<(&serde_json::Value, &serde_json::Value)> {
        match self {
            Self::NeedEsQuery { header, query, .. } => Some((header, query)),
            _ => None,
        }
    }

    pub fn into_value(self) -> Option<T> {
        match self {
            Self::Value(x) => Some(x),
            _ => None,
        }
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
        .filter(|partial| partial.value().is_none())
        .collect();

    info!("sending {} requests to ES", need_progress.len());

    let body: String = {
        need_progress
            .iter()
            .filter_map(|partial| {
                let (header, query) = partial.header_and_query()?;
                Some(format!("{}\n{}\n", header.to_string(), query.to_string()))
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
            LazyEs::Value(_) => unreachable!(),
        }
    }
}

pub fn batch_make_progress_until_value<'a, T: 'a>(
    rubber: &mut Rubber,
    mut partials: Vec<LazyEs<'a, T>>,
) -> Vec<T> {
    while partials.iter().any(|x| x.value().is_none()) {
        batch_make_progress(rubber, partials.as_mut_slice());
    }

    partials
        .into_iter()
        .map(|partial| partial.into_value().unwrap())
        .collect()
}

// ---
// --- Lower level ES interractions
// ---

#[derive(Deserialize)]
pub struct EsResponse<U> {
    pub hits: EsHits<U>,
}

#[derive(Deserialize)]
pub struct EsHits<U> {
    pub hits: Vec<EsHit<U>>,
}

#[derive(Deserialize)]
pub struct EsHit<U> {
    #[serde(rename = "_source")]
    pub source: U,
    #[serde(rename = "_type")]
    pub doc_type: String,
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
