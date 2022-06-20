use std::path::PathBuf;

use futures::Future;
use mimir::common::config::config_from;
use mimirsbrunn::utils::logger::logger_init;
use serde::de::DeserializeOwned;
use structopt::StructOpt;
use tracing::info;

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

pub async fn run<S: DeserializeOwned, R: Future>(f: impl FnOnce(S) -> R) -> R::Output {
    let _log_guard = logger_init().expect("could not init logger");
    let args = Args::from_args();

    let raw_config = config_from(
        &args.config_dir,
        &["elasticsearch", "fafnir"],
        args.run_mode.as_deref(),
        "MIMIR",
        args.settings,
    )
    .expect("could not build fafnir config");

    info!(
        "Full configuration:\n{}",
        serde_json::to_string_pretty(
            &raw_config
                .clone()
                .try_deserialize::<serde_json::Value>()
                .expect("could not convert config to json"),
        )
        .expect("could not serialize config"),
    );

    let settings: S = raw_config.try_deserialize().expect("invalid fafnir config");
    f(settings).await
}
