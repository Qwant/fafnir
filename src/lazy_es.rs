use elasticsearch::http::request::JsonBody;
use elasticsearch::{Elasticsearch, MsearchParts};
use serde::Deserialize;
use serde_json::value::RawValue;

// ---
// --- LazyEs
// ---

/// Computation result that may lazily rely on an elasticsearch "search"
/// request.
pub enum LazyEs<'p, T> {
    /// The result of the computation is ready.
    Value(T),
    /// The computation needs to make a request to elasticsearch in order to
    /// make progress, the function `progress` takes the raw answer from
    /// elasticsearch and returns the new state of the computation.
    NeedEsQuery {
        // TODO: Isn't RawValue enough ?
        header: serde_json::Value,
        query: serde_json::Value,
        progress: Box<dyn FnOnce(&str) -> LazyEs<'p, T> + 'p>,
    },
}

impl<'p, T: 'p> LazyEs<'p, T> {
    /// Read computed value if it is ready.
    pub fn value(&self) -> Option<&T> {
        match self {
            Self::Value(x) => Some(x),
            _ => None,
        }
    }

    /// Extract computed value if it is ready.
    pub fn into_value(self) -> Option<T> {
        match self {
            Self::Value(x) => Some(x),
            _ => None,
        }
    }

    /// Read the header and query that have to be sent to elasticsearch if it
    /// is required.
    pub fn header_and_query(&self) -> Option<(&serde_json::Value, &serde_json::Value)> {
        match self {
            Self::NeedEsQuery { header, query, .. } => Some((header, query)),
            _ => None,
        }
    }

    /// Chain some computation out of the value which will eventually be
    /// computed.
    pub fn map<U>(self, func: impl FnOnce(T) -> U + 'p) -> LazyEs<'p, U> {
        self.then(move |x| LazyEs::Value(func(x)))
    }

    /// Chain some lazy computation out of the value which will eventually be
    /// computed. This means that one more elasticsearch request may be
    /// required to compute the final result.
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

    /// Send a request to elasticsearch to make progress for all computations
    /// in `partials` that are not done yet.
    async fn batch_make_progress(
        es: &Elasticsearch,
        partials: &mut [Self],
        max_batch_size: usize,
    ) -> usize {
        let need_progress: Vec<_> = partials
            .iter_mut()
            .filter(|partial| partial.value().is_none())
            .take(max_batch_size)
            .collect();

        if need_progress.is_empty() {
            return 0;
        }

        let body: Vec<_> = {
            need_progress
                .iter()
                .flat_map(|partial| {
                    partial
                        .header_and_query()
                        .map(|(header, query)| [header, query])
                })
                .flatten()
                .map(JsonBody::new)
                .collect()
        };

        let es_response = es
            .msearch(MsearchParts::None)
            .body(body)
            .send()
            .await
            .expect("ES query failed")
            .text()
            .await
            .expect("failed to read ES response");

        let responses = parse_es_multi_response(&es_response)
            .unwrap_or_else(|err| panic!("failed to parse ES responses: {}\n{}", err, es_response));

        assert_eq!(responses.len(), need_progress.len());
        let progress_count = need_progress.len();

        for (partial, res) in need_progress.into_iter().zip(responses) {
            match partial {
                LazyEs::NeedEsQuery { progress, .. } => {
                    let progress = std::mem::replace(progress, Box::new(|_| unreachable!()));
                    *partial = progress(res);
                }
                LazyEs::Value(_) => unreachable!(),
            }
        }

        progress_count
    }

    /// Run all input computations until they are finished and finally output
    /// the resulting values.
    pub async fn batch_make_progress_until_value(
        es: &Elasticsearch,
        mut partials: Vec<Self>,
        max_batch_size: usize,
    ) -> Vec<T> {
        // Don't stop while some progress has been made during the loop condition.
        while Self::batch_make_progress(es, &mut partials, max_batch_size).await > 0 {}

        partials
            .into_iter()
            .map(|partial| partial.into_value().expect("some tasks are not finished"))
            .collect()
    }
}

// ---
// --- Elasticsearch response structure
// ---

#[derive(Deserialize)]
struct EsResponse<'a, U> {
    hits: Option<EsHits<U>>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
}

#[derive(Deserialize)]
pub struct EsHits<U> {
    pub hits: Vec<EsHit<U>>,
}

#[derive(Deserialize)]
pub struct EsHit<U> {
    #[serde(rename = "_source")]
    pub source: U,
}

// ---
// --- Elasticsearch utils
// ---

#[derive(Debug)]
pub enum EsError<'a> {
    Es(&'a str),
    MissingFields(&'a str),
    Parsing(serde_json::Error),
}

pub fn parse_es_multi_response(es_multi_response: &str) -> serde_json::Result<Vec<&str>> {
    #[derive(Deserialize)]
    struct EsResponse<'a> {
        #[serde(borrow)]
        responses: Vec<&'a RawValue>,
    }

    let es_response: EsResponse = serde_json::from_str(es_multi_response)?;

    Ok(es_response
        .responses
        .into_iter()
        .map(RawValue::get)
        .collect())
}

pub fn parse_es_response<'a, U: Deserialize<'a>>(
    es_response: &'a str,
) -> Result<Vec<EsHit<U>>, EsError<'a>> {
    match serde_json::from_str(es_response).map_err(EsError::Parsing)? {
        EsResponse {
            hits: _,
            error: Some(err),
        } => Err(EsError::Es(err.get())),
        EsResponse {
            hits: Some(hits),
            error: _,
        } => Ok(hits.hits),
        _ => Err(EsError::MissingFields(es_response)),
    }
}
