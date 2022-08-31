use std::fmt;
use std::io::Read;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;

use elasticsearch::cat::CatIndicesParts;
use elasticsearch::Elasticsearch;
use serde::de::{Deserializer, SeqAccess, Visitor};
use serde_json::value::RawValue;
use tracing::warn;

/// Max number of buffered JSON objects in a streaming iterator over an array.
const BUFFERED_JSON_OBJS: usize = 64;

pub async fn start_postgres_session(
    config: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(config, tokio_postgres::NoTls).await?;

    // The connection object performs the actual communication with the database
    // and must be spawned inside of tokio.
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            panic!("Postgres connection error: {err}");
        }
    });

    Ok(client)
}

/// Get creation date of an index as a timestamp.
pub async fn get_index_creation_date(es: &Elasticsearch, index: impl AsRef<str>) -> Option<u64> {
    let res = es
        .cat()
        .indices(CatIndicesParts::Index(&[index.as_ref()]))
        .h(&["creation.date"])
        .send()
        .await
        .map_err(|err| warn!("failed to query ES for creation date: {err:?}"))
        .ok()?;

    let raw = res
        .text()
        .await
        .map_err(|err| warn!("failed to load ES response for creation date: {err:?}"))
        .ok()?;

    if raw.is_empty() {
        return None;
    }

    raw.trim()
        .parse()
        .map_err(|err| warn!("invalid index creation timestamp: {err:?}"))
        .ok()
}

/// Spawn a thread that will deserialize the input JSON reader and return an iterator over the raw
/// JSON values yielded in streaming.
///
/// Inspired from https://serde.rs/stream-array.html
pub fn json_array_iter(reader: impl Read + Send + 'static) -> impl Iterator<Item = Box<RawValue>> {
    // Define a custom visitor that fills a channel instead of outputing the result in a local
    // variable.
    struct ArrayVisitor(SyncSender<Box<RawValue>>);

    impl<'de> Visitor<'de> for ArrayVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a sequence")
        }

        fn visit_seq<S>(self, mut seq: S) -> Result<(), S::Error>
        where
            S: SeqAccess<'de>,
        {
            while let Some(value) = seq.next_element()? {
                if let Err(_) = self.0.send(value) {
                    // Error is ignored because it just means that the caller dropped the returned
                    // iterator.
                    break;
                }
            }

            Ok(())
        }
    }

    let (send, recv) = sync_channel(BUFFERED_JSON_OBJS);

    // Spawn a thread that will iterate over the JSON sequence and fill the channel.
    thread::Builder::new()
        .name("streaming JSON iterator".to_string())
        .spawn(move || {
            let visitor = ArrayVisitor(send);
            let mut de = serde_json::Deserializer::from_reader(reader);
            de.deserialize_seq(visitor).expect("deserialization error");
        })
        .expect("failed to start thread");

    recv.into_iter()
}
