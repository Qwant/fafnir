extern crate fafnir;
extern crate mimirsbrunn;
extern crate postgres;
#[macro_use]
extern crate structopt;
extern crate num_cpus;

#[derive(StructOpt, Debug)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
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
    /// Bounding box to filter the imported pois
    /// The format is "lat1, lon1, lat2, lon2"
    #[structopt(short = "b", long = "bounding-box")]
    bounding_box: Option<String>,
    /// Number of shards for the es index
    #[structopt(short = "s", long = "nb-shards", default_value = "1")]
    nb_shards: usize,
    /// Number of replicas for the es index
    #[structopt(short = "r", long = "nb-replicas", default_value = "1")]
    nb_replicas: usize,
    /// Languages codes, used to build i18n names and labels
    #[structopt(name = "lang", short, long)]
    langs: Vec<String>,
}

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let client = postgres::Client::connect(&args.pg, postgres::tls::NoTls).unwrap_or_else(|err| {
        panic!("Unable to connect to postgres: {}", err);
    });

    let dataset = args.dataset;
    let nb_threads = args.nb_threads.unwrap_or_else(num_cpus::get);
    fafnir::load_and_index_pois(
        args.es,
        client,
        dataset,
        nb_threads,
        args.bounding_box,
        args.nb_shards,
        args.nb_replicas,
        args.langs,
    )
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
