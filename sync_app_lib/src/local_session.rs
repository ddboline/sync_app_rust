use anyhow::{format_err, Error};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::{io::AsyncWriteExt, process::Command};

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

    /// # Errors
    /// Return error if db query fails
    pub async fn run_command_export(
        &self,
        table: &str,
        start_timestamp: Option<OffsetDateTime>,
    ) -> Result<Vec<u8>, Error> {
        let mut args = vec!["export", "-t", table];
        let start_timestamp = start_timestamp.and_then(|t| t.format(&Rfc3339).ok());
        if let Some(start_timestamp) = start_timestamp.as_ref() {
            args.push("-s");
            args.push(start_timestamp);
        }
        self.run_command(&args).await
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn run_command(&self, args: &[&str]) -> Result<Vec<u8>, Error> {
        let output = Command::new(&self.exe_path)
            .env_remove("DATABASE_URL")
            .env_remove("PGURL")
            .args(args)
            .kill_on_drop(true)
            .output()
            .await?;
        if output.status.success() {
            Ok(output.stdout)
        } else {
            Err(format_err!(
                "Process returned {:?} {:?}",
                output.status,
                output.stderr
            ))
        }
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn run_command_import(
        &self,
        table: &str,
        input: &[u8],
        start_timestamp: Option<OffsetDateTime>,
    ) -> Result<Vec<u8>, Error> {
        let mut args = vec!["import", "-t", table];
        let start_timestamp = start_timestamp.and_then(|t| t.format(&Rfc3339).ok());
        if let Some(start_timestamp) = start_timestamp.as_ref() {
            args.push("-s");
            args.push(start_timestamp);
        }
        let mut child = Command::new(&self.exe_path)
            .env_remove("DATABASE_URL")
            .env_remove("PGURL")
            .args(&args)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdin = child.stdin.take();
        if let Some(mut stdin) = stdin {
            stdin.write_all(input).await?;
        }
        let output = child.wait_with_output().await?;
        if output.status.success() {
            Ok(output.stdout)
        } else {
            Err(format_err!(
                "Process returned {:?} {:?}",
                output.status,
                output.stderr
            ))
        }
    }
}
