use std::fmt;
use std::time::Duration;

use elasticsearch::http::request::JsonBody;
use elasticsearch::{Elasticsearch, MsearchParts};
use futures::lock::Mutex;
use mimir::utils::futures::with_backoff;
use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::warn;

const BACKOFF_RETRIES: u8 = 6;
const BACKOFF_DELAY: Duration = Duration::from_secs(1);

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
        progress: Box<dyn FnOnce(Vec<EsHit<&RawValue>>) -> LazyEs<'p, T> + 'p + Send>,
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
    pub fn map<U>(self, func: impl FnOnce(T) -> U + Send + 'p) -> LazyEs<'p, U> {
        self.then(move |x| LazyEs::Value(func(x)))
    }

    /// Chain some lazy computation out of the value which will eventually be
    /// computed. This means that one more elasticsearch request may be
    /// required to compute the final result.
    pub fn then<U>(self, func: impl FnOnce(T) -> LazyEs<'p, U> + Send + 'p) -> LazyEs<'p, U> {
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
    async fn batch_make_progress<'a>(
        es: &Elasticsearch,
        partials: &mut [Self],
        max_batch_size: usize,
    ) -> Result<usize, EsError> {
        let need_progress: Vec<_> = partials
            .iter_mut()
            .filter(|partial| partial.value().is_none())
            .take(max_batch_size)
            .collect();

        if need_progress.is_empty() {
            return Ok(0);
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

        let es_request = with_backoff(
            || {
                es.msearch(MsearchParts::None)
                    .body(body.iter().collect())
                    .send()
            },
            BACKOFF_RETRIES,
            BACKOFF_DELAY,
        );

        let es_response = es_request
            .await
            .expect("ES query failed")
            .text()
            .await
            .expect("failed to read ES response");

        let need_progress_len = need_progress.len();
        let responses = parse_es_multi_response(&es_response).map_err(EsError::Parsing)?;
        assert_eq!(responses.len(), need_progress_len);

        let mut progress_count = 0;
        let mut errors = Vec::new();

        for (partial, res) in need_progress.into_iter().zip(responses) {
            match res.into_hits() {
                Ok(hits) => {
                    let progress = match partial {
                        LazyEs::Value(_) => unreachable!(),
                        LazyEs::NeedEsQuery { progress, .. } => progress,
                    };

                    let progress = std::mem::replace(progress, Box::new(|_| unreachable!()));
                    *partial = progress(hits);
                    progress_count += 1;
                }
                Err(err) => errors.push(err),
            }
        }

        if errors.len() > 1 {
            warn!(
                "got {}/{need_progress_len} errors during bulk progress",
                errors.len(),
            );
        }

        if let Some(err) = errors.into_iter().next() {
            Err(err)
        } else {
            Ok(progress_count)
        }
    }

    /// Run all input computations until they are finished and finally output
    /// the resulting values.
    pub async fn batch_make_progress_until_value(
        es: &Elasticsearch,
        partials: Vec<Self>,
        max_batch_size: usize,
    ) -> Vec<T> {
        // `partials` needs to be wrapped with a `Mutex` (would be a `RefCell` in a single threaded
        // context) because the closure `make_progress` will return a future containing a mutable
        // reference to it. Hence we need to ensure at runtime that this closure won't be called
        // twice without consuming the future first.
        let partials = Mutex::new(partials);

        let make_progress = || async {
            let mut partials = partials
                .try_lock()
                .expect("`make_progress` was called concurrently");

            Self::batch_make_progress(es, partials.as_mut(), max_batch_size).await
        };

        // Don't stop while some progress has been made during the loop condition.
        while with_backoff(make_progress, BACKOFF_RETRIES, BACKOFF_DELAY)
            .await
            .expect("exceeded number of retries for batch progress")
            > 0
        {}

        partials
            .into_inner()
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

impl<'a, U> EsResponse<'a, U> {
    fn into_hits(self) -> Result<Vec<EsHit<U>>, EsError> {
        match self {
            EsResponse {
                hits: _,
                error: Some(err),
            } => Err(EsError::Es(err.to_owned())),
            EsResponse {
                hits: Some(hits),
                error: _,
            } => Ok(hits.hits),
            _ => Err(EsError::MissingFields(&["hits", "error"])),
        }
    }
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
pub enum EsError {
    Es(Box<RawValue>),
    MissingFields(&'static [&'static str]),
    Parsing(serde_json::Error),
}

impl fmt::Display for EsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EsError::Es(inner) => write!(f, "ES error: {}", inner),
            EsError::MissingFields(fields) => write!(f, "missing expected fields: {:?}", fields),
            EsError::Parsing(inner) => write!(f, "parsing error: {}", inner),
        }
    }
}

fn parse_es_multi_response<'a, U: Deserialize<'a>>(
    es_multi_response: &'a str,
) -> serde_json::Result<Vec<EsResponse<'a, U>>> {
    #[derive(Deserialize)]
    struct EsResponses<'a, U> {
        #[serde(borrow)]
        responses: Vec<EsResponse<'a, U>>,
    }

    let res: EsResponses<'a, U> = serde_json::from_str(es_multi_response)?;
    Ok(res.responses)
}
