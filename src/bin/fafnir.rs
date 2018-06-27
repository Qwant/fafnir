extern crate fafnir;
extern crate mimirsbrunn;
extern crate postgres;
#[macro_use]
extern crate structopt;
extern crate num_cpus;

#[derive(StructOpt, Debug)]
#[structopt(raw(setting = "structopt::clap::AppSettings::ColoredHelp"))]
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
    /// Number of threads used. The default is to use the number of cpus
    #[structopt(short = "n", long = "nb-threads")]
    nb_threads: Option<usize>,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let conn =
        postgres::Connection::connect(args.pg, postgres::TlsMode::None).unwrap_or_else(|err| {
            panic!("Unable to connect to postgres: {}", err);
        });

    let dataset = args.dataset;
    let nb_threads = args.nb_threads.unwrap_or(num_cpus::get());
    fafnir::load_and_index_pois(args.es, conn, dataset, nb_threads);
    Ok(())
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
