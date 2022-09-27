use fafnir::settings::PostgresSettings;
use fafnir::utils::start_postgres_session;
use std::error::Error;
use std::process::Command;
use std::time::Duration;
use tracing::{info, warn};

pub struct PostgresDocker {
    ip: String,
}

impl PostgresDocker {
    pub async fn new() -> Result<PostgresDocker, Box<dyn Error>> {
        let mut pg_docker = PostgresDocker { ip: "".to_string() };
        pg_docker.setup().await?;
        Ok(pg_docker)
    }

    pub fn host(&self) -> String {
        self.ip.to_string()
    }

    pub async fn setup(&mut self) -> Result<(), Box<dyn Error>> {
        info!("Launching the PostgresWrapper docker");
        let (name, img) = ("postgres_fafnir_tests", "openmaptiles/postgis");

        let status = Command::new("docker")
            .args(&[
                "run",
                "--env",
                "POSTGRES_DB=test",
                "--env",
                "POSTGRES_USER=test",
                "--env",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-P",
                "-d",
                &format!("--name={name}"),
                img,
            ])
            .status()?;
        if !status.success() {
            return Err(format!("`docker run` failed {status}").into());
        }

        // we need to get the ip of the container if the container has been run on another machine
        let container_ip_cmd = Command::new("docker")
            .args(&["inspect", "--format={{.NetworkSettings.IPAddress}}", name])
            .output()?;

        let container_ip = ::std::str::from_utf8(container_ip_cmd.stdout.as_slice())?.trim();

        info!("container ip = {container_ip:?}");
        self.ip = container_ip.to_string();
        info!("Waiting for Postgres in docker to be up and running...");

        let mut retries = 0;

        while start_postgres_session(PostgresSettings {
            host: (&self.host()).to_string(),
            port: 5432,
            user: "test".to_string(),
            password: "".to_string(),
            database: "test".to_string(),
        })
        .await
        .is_err()
        {
            retries += 1;

            if retries > 60 {
                return Err("Postgres is down".into());
            }

            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        info!("{name} docker is up and running");
        Ok(())
    }
}

fn docker_command(args: &[&'static str]) {
    info!("Running docker {args:?}");
    let status = Command::new("docker").args(args).status();
    match status {
        Ok(s) => {
            if !s.success() {
                warn!("`docker {args:?}` failed {s}")
            }
        }
        Err(e) => warn!("command `docker {args:?}` failed {e}"),
    }
}

impl Drop for PostgresDocker {
    fn drop(&mut self) {
        if ::std::env::var("DONT_KILL_THE_WHALE") == Ok("1".to_string()) {
            warn!("the postgres docker won't be stoped at the end, you can debug it.");
            return;
        }
        docker_command(&["stop", "postgres_fafnir_tests"]);
        docker_command(&["rm", "postgres_fafnir_tests"]);
    }
}
