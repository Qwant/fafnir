extern crate fafnir;
extern crate mimirsbrunn;
extern crate postgres;
#[macro_use]
extern crate structopt;

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
    /// Number of threads used
    #[structopt(short = "n", long = "nb-threads", default_value="2")]
    nb_threads: usize,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let conn =
        postgres::Connection::connect(args.pg, postgres::TlsMode::None).unwrap_or_else(|err| {
            panic!("Unable to connect to postgres: {}", err);
        });

    let dataset = args.dataset;
    fafnir::load_and_index_pois(args.es, conn, dataset, args.nb_threads);
    Ok(())
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
