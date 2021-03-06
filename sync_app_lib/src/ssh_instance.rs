use anyhow::{format_err, Error};
use lazy_static::lazy_static;
use log::debug;
use smallvec::{smallvec, SmallVec};
use std::{collections::HashMap, process::Stdio};
use tokio::{
    io::{stdout, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, RwLock},
    task::spawn,
};
use url::Url;

lazy_static! {
    static ref LOCK_CACHE: RwLock<HashMap<String, Mutex<()>>> = RwLock::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct SSHInstance {
    pub user: String,
    pub host: String,
    pub port: u16,
}

impl SSHInstance {
    pub async fn new(user: &str, host: &str, port: u16) -> Self {
        LOCK_CACHE
            .write()
            .await
            .insert(host.to_string(), Mutex::new(()));
        Self {
            user: user.to_string(),
            host: host.to_string(),
            port,
        }
    }

    pub async fn from_url(url: &Url) -> Result<Self, Error> {
        let host = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        Ok(Self::new(user, host, port).await)
    }

    pub fn get_ssh_str(&self, path: &str) -> Result<String, Error> {
        let ssh_str = if self.port == 22 {
            format!("{}@{}:{}", self.user, self.host, path)
        } else {
            format!("-p {} {}@{}:{}", self.port, self.user, self.host, path)
        };

        Ok(ssh_str)
    }

    pub fn get_ssh_username_host(&self) -> Result<SmallVec<[String; 3]>, Error> {
        let ssh_str = if self.port == 22 {
            smallvec![format!("{}@{}", self.user, self.host)]
        } else {
            smallvec![
                "-p".to_string(),
                self.port.to_string(),
                format!("{}@{}", self.user, self.host)
            ]
        };

        Ok(ssh_str)
    }

    pub async fn run_command_stream_stdout(&self, cmd: &str) -> Result<String, Error> {
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock().await;
            debug!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let mut args: SmallVec<[&str; 4]> = user_host.iter().map(String::as_str).collect();
            args.push(cmd);
            let process = Command::new("ssh").args(&args).output().await?;
            String::from_utf8(process.stdout).map_err(Into::into)
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub async fn run_command_print_stdout(&self, cmd: &str) -> Result<(), Error> {
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock();
            debug!("run_command_print_stdout cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let mut args: SmallVec<[&str; 4]> = user_host.iter().map(String::as_str).collect();
            args.push(cmd);
            let mut command = Command::new("ssh")
                .args(&args)
                .stdout(Stdio::piped())
                .spawn()?;

            let stdout_handle = command
                .stdout
                .take()
                .ok_or_else(|| format_err!("No stdout"))?;
            let mut reader = BufReader::new(stdout_handle);

            let mut line = String::new();
            let mut stdout = stdout();
            while let Ok(bytes) = reader.read_line(&mut line).await {
                if bytes > 0 {
                    let user_host = &user_host[user_host.len() - 1];
                    stdout
                        .write_all(format!("ssh://{}{}", user_host, line).as_bytes())
                        .await?;
                } else {
                    break;
                }
                line.clear();
            }
            command.wait().await?;
        }
        Ok(())
    }

    pub async fn run_command_ssh(&self, cmd: &str) -> Result<(), Error> {
        let user_host = self.get_ssh_username_host()?;
        let mut args: SmallVec<[&str; 4]> = user_host.iter().map(String::as_str).collect();
        args.push(cmd);
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock().await;
            debug!("run_command_ssh cmd {}", cmd);
            if Command::new("ssh").args(&args).status().await?.success() {
                Ok(())
            } else {
                Err(format_err!("{} failed", cmd))
            }
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<(), Error> {
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock();
            debug!("cmd {} {}", cmd, args.join(" "));
            if Command::new(cmd).args(args).status().await?.success() {
                Ok(())
            } else {
                Err(format_err!("{} {} failed", cmd, args.join(" ")))
            }
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub async fn run_scp(&self, arg0: &str, arg1: &str) -> Result<(), Error> {
        self.run_command("scp", &[arg0, arg1]).await
    }
}
