use log::warn;
use mimir::rubber::Rubber;

pub async fn start_postgres_session(
    config: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::connect(config, tokio_postgres::NoTls).await?;

    // The connection object performs the actual communication with the database
    // and must be spawned inside of tokio.
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            panic!("Postgres connection error: {}", err);
        }
    });

    Ok(client)
}

/// Get creation date of an index as a timestamp.
pub fn get_index_creation_date(rubber: &mut Rubber, index: &str) -> Option<u64> {
    let query = format!("/_cat/indices/{}?h=creation.date", index);

    rubber
        .get(&query)
        .map_err(|err| warn!("could not query ES: {:?}", err))
        .ok()
        .and_then(|res| {
            res.text()
                .map_err(|err| warn!("could not load ES response: {:?}", err))
                .ok()
        })
        .and_then(|text| {
            text.trim()
                .parse()
                .map_err(|err| warn!("invalid index creation timestamp: {:?}", err))
                .ok()
        })
}
