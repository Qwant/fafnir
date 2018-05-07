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
    #[structopt(long = "es", default_value = "http://localhost:9200/")]
    es: String,
    /// Name of the dataset.
    #[structopt(short = "d", long = "dataset")]
    dataset: String,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let conn =
        postgres::Connection::connect(args.pg, postgres::TlsMode::None).unwrap_or_else(|err| {
            panic!("Unable to connect to postgres: {}", err);
        });

    let mut rubber = mimir::rubber::Rubber::new(&args.es);
    let dataset = &args.dataset;
    fafnir::load_and_index_pois(&args.es, &mut rubber, &conn, dataset);
    Ok(())
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
