use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::{Child, Command};

#[derive(Clone)]
pub struct LocalSession {
    exe_path: PathBuf,
}

impl LocalSession {
    pub fn new<T: AsRef<Path>>(exe_path: T) -> Self {
        let exe_path = if exe_path.as_ref().exists() {
            exe_path.as_ref().to_path_buf()
        } else {
            Path::new("/usr/bin").join(exe_path)
        };
        Self { exe_path }
    }

    async fn run_command<T: AsRef<Path>>(
        &self,
        command: &str,
        table: &str,
        output: T,
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let output = output.as_ref().to_string_lossy();
        let mut args = vec![command, "-t", table, "-f", output.as_ref()];
        let start_timestamp = start_timestamp.map(|t| t.to_rfc3339());
        if let Some(start_timestamp) = start_timestamp.as_ref() {
            args.push("-s");
            args.push(&start_timestamp);
        }
        let status = Command::new(&self.exe_path)
            .env_remove("DATABASE_URL")
            .env_remove("PGURL")
            .args(&args)
            .kill_on_drop(true)
            .status()
            .await?;
        if status.success() {
            Ok(())
        } else {
            Err(format_err!("Process returned {:?}", status))
        }
    }

    pub async fn export<T: AsRef<Path>>(
        &self,
        table: &str,
        input: T,
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.run_command("export", table, input, start_timestamp)
            .await
    }

    pub async fn import<T: AsRef<Path>>(
        &self,
        table: &str,
        input: T,
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.run_command("import", table, input, start_timestamp)
            .await
    }
}
