use std::path::PathBuf;

use mimir2::common::config::config_from;
use structopt::StructOpt;
use tracing::error;

use fafnir::openmaptiles2mimir::load_and_index_pois;

#[derive(StructOpt, Debug)]
#[structopt(setting = structopt::clap::AppSettings::ColoredHelp)]
struct Args {
    /// Defines the config directories
    #[structopt(parse(from_os_str), short = "c", long = "config-dir")]
    pub config_dir: PathBuf,

    /// Defines the run mode in {testing, dev, prod, ...}
    ///
    /// If no run mode is provided, a default behavior will be used.
    #[structopt(short = "m", long = "run-mode")]
    pub run_mode: Option<String>,

    /// Override settings values using key=value
    #[structopt(short = "s", long = "setting")]
    pub settings: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();

    let config = config_from(
        &args.config_dir,
        &["elasticsearch", "fafnir", "logging"],
        args.run_mode.as_deref(),
        "MIMIR",
        args.settings,
    )
    .expect("could not build fafnir config");

    if let Err(err) = load_and_index_pois(config).await {
        error!("Error while running fafnir: {}", err)
    }
}
