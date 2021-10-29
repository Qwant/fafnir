use structopt::StructOpt;

use fafnir::utils::start_postgres_session;
use fafnir::Args;

async fn run(args: Args) -> Result<(), mimirsbrunn::Error> {
    let client = start_postgres_session(&args.pg)
        .await
        .unwrap_or_else(|err| panic!("Unable to connect to postgres: {}", err));
    let nb_threads = args.nb_threads.unwrap_or_else(num_cpus::get);
    fafnir::load_and_index_pois(client, nb_threads, args).await
}

#[tokio::main]
async fn main() {
    // TODO: previously errors were displayed using mimirsbrunn's `launch_run` method.
    if let Err(err) = run(Args::from_args()).await {
        dbg!(err);
    }
}
