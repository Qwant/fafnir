use crate::settings::PostgresSettings;
use elasticsearch::cat::CatIndicesParts;
use elasticsearch::Elasticsearch;
use tokio_postgres::{Config, NoTls};
use tracing::warn;

pub async fn start_postgres_session(
    settings: PostgresSettings,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = Config::new()
        .host(&settings.host)
        .user(&settings.user)
        .port(settings.port)
        .password(&settings.password)
        .dbname(&settings.database)
        .connect(NoTls)
        .await?;

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
