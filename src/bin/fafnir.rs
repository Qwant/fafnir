use fafnir::Args;
use log::LevelFilter;

fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    log::set_max_level(LevelFilter::Info);
    let client = postgres::Client::connect(&args.pg, postgres::tls::NoTls).unwrap_or_else(|err| {
        panic!("Unable to connect to postgres: {}", err);
    });
    let nb_threads = args.nb_threads.unwrap_or_else(num_cpus::get);
    fafnir::load_and_index_pois(client, nb_threads, args)
}

fn main() {
    mimirsbrunn::utils::launch_run(run);
}
