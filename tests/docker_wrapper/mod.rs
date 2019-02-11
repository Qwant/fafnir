extern crate retry;

use super::hyper;
use mimir::rubber::Rubber;
use postgres::{Connection, TlsMode};
use std::error::Error;
use std::process::Command;

pub struct ElasticsearchDocker {
    ip: String,
}

pub struct PostgresDocker {
    ip: String,
}

impl PostgresDocker {
    pub fn new() -> Result<PostgresDocker, Box<Error>> {
        let mut pg_docker = PostgresDocker { ip: "".to_string() };
        try!(pg_docker.setup());
        Ok(pg_docker)
    }

    pub fn host(&self) -> String {
        format!("{}", self.ip)
    }

    pub fn setup(&mut self) -> Result<(), Box<Error>> {
        info!("Launching the PostgresWrapper docker");
        let (name, img) = ("postgres_fafnir_tests", "openmaptiles/postgis");

        let status = try!(Command::new("docker")
            .args(&[
                "run",
                "--env",
                "POSTGRES_DB=test",
                "--env",
                "POSTGRES_USER=test",
                "-P",
                "-d",
                &format!("--name={}", name),
                img,
            ])
            .status());
        if !status.success() {
            return Err(format!("`docker run` failed {}", &status).into());
        }

        // we need to get the ip of the container if the container has been run on another machine
        let container_ip_cmd = try!(Command::new("docker")
            .args(&["inspect", "--format={{.NetworkSettings.IPAddress}}", name])
            .output());

        let container_ip = ::std::str::from_utf8(container_ip_cmd.stdout.as_slice())?.trim();

        info!("container ip = {:?}", container_ip);
        self.ip = container_ip.to_string();

        info!("Waiting for Postgres in docker to be up and running...");

        let retry = retry::retry(
            200,
            700,
            || {
                Connection::connect(
                    format!("postgres://test@{}/test", &self.host()),
                    TlsMode::None,
                )
            },
            |connection| connection.is_ok(),
        );
        match retry {
            Ok(_) => {
                info!("{} docker is up and running", name);
                return Ok(());
            }
            Err(_) => return Err("Postgres is down".into()),
        }
    }
}

impl ElasticsearchDocker {
    pub fn new() -> Result<ElasticsearchDocker, Box<Error>> {
        let mut el_docker = ElasticsearchDocker { ip: "".to_string() };
        try!(el_docker.setup());
        let rubber = Rubber::new(&el_docker.host());
        &rubber.initialize_templates().unwrap();
        Ok(el_docker)
    }

    pub fn host(&self) -> String {
        format!("http://{}:9200", self.ip)
    }

    pub fn setup(&mut self) -> Result<(), Box<Error>> {
        info!("Launching docker");
        let (name, img) = ("mimirsbrunn_fafnir_tests", "elasticsearch:2");

        let status = try!(Command::new("docker")
            .args(&["run", "-d", &format!("--name={}", name), img])
            .status());
        if !status.success() {
            return Err(format!("`docker run` failed {}", &status).into());
        }

        // we need to get the ip of the container if the container has been run on another machine
        let container_ip_cmd = try!(Command::new("docker")
            .args(&["inspect", "--format={{.NetworkSettings.IPAddress}}", name])
            .output());

        let container_ip = ::std::str::from_utf8(container_ip_cmd.stdout.as_slice())?.trim();

        info!("container ip = {:?}", container_ip);
        self.ip = container_ip.to_string();

        info!("Waiting for ES in docker to be up and running...");
        let retry = retry::retry(
            200,
            100,
            || hyper::client::Client::new().get(&self.host()).send(),
            |response| {
                response
                    .as_ref()
                    .map(|res| res.status == hyper::Ok)
                    .unwrap_or(false)
            },
        );
        match retry {
            Ok(_) => {
                info!("{} docker is up and running", name);
                return Ok(());
            }
            Err(_) => return Err("ElasticSearch is down".into()),
        };
    }
}

fn docker_command(args: &[&'static str]) {
    info!("Running docker {:?}", args);
    let status = Command::new("docker").args(args).status();
    match status {
        Ok(s) => {
            if !s.success() {
                warn!("`docker {:?}` failed {}", args, s)
            }
        }
        Err(e) => warn!("command `docker {:?}` failed {}", args, e),
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

impl Drop for ElasticsearchDocker {
    fn drop(&mut self) {
        if ::std::env::var("DONT_KILL_THE_WHALE") == Ok("1".to_string()) {
            warn!("the elasticsearch docker won't be stoped at the end, you can debug it.");
            return;
        }
        docker_command(&["stop", "mimirsbrunn_fafnir_tests"]);
        docker_command(&["rm", "mimirsbrunn_fafnir_tests"]);
    }
}
