extern crate fafnir;
extern crate postgres;
#[macro_use]
extern crate structopt;
extern crate mimir;
extern crate mimirsbrunn;

#[derive(StructOpt, Debug)]
struct Args {
    /// Postgresql parameters
    #[structopt(long = "pg")]
    pg: String,
    /// Elasticsearch parameters.
    #[structopt(long = "connection-string", default_value = "http://localhost:9200/")]
    connection_string: String,
    /// Name of the dataset.
    #[structopt(short = "d", long = "dataset")]
    dataset: String,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let conn =
        postgres::Connection::connect(args.pg, postgres::TlsMode::None).unwrap_or_else(|err| {
            panic!("Unable to connect to postgres: {}", err);
        });

    let rubber = mimir::rubber::Rubber::new(&args.connection_string);
    let dataset = &args.dataset;
    fafnir::load_and_index_pois(rubber, &conn, dataset);
    Ok(())
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
