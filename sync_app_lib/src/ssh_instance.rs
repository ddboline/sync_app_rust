use anyhow::{format_err, Error};
use lazy_static::lazy_static;
use log::{debug, info, error};
use smallvec::{smallvec, SmallVec};
use std::{collections::HashMap, process::Stdio};
use tokio::{
    io::{stdout, AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, RwLock},
    task::spawn,
};
use url::Url;
use stack_string::StackString;
use std::fmt::Write;

lazy_static! {
    static ref LOCK_CACHE: RwLock<HashMap<StackString, Mutex<()>>> = RwLock::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct SSHInstance {
    pub user: StackString,
    pub host: StackString,
    pub port: u16,
}

impl SSHInstance {
    pub async fn new(user: &str, host: &str, port: u16) -> Self {
        LOCK_CACHE
            .write()
            .await
            .insert(host.into(), Mutex::new(()));
        Self {
            user: user.into(),
            host: host.into(),
            port,
        }
    }

    pub async fn from_url(url: &Url) -> Result<Self, Error> {
        let host = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        Ok(Self::new(user, host, port).await)
    }

    pub fn get_ssh_str(&self, path: &str) -> Result<StackString, Error> {
        let mut ssh_str = StackString::new();
        if self.port == 22 {
            write!(ssh_str, "{}@{}:{}", self.user, self.host, path)?;
        } else {
            write!(ssh_str, "-p {} {}@{}:{}", self.port, self.user, self.host, path)?;
        };

        Ok(ssh_str)
    }

    pub fn get_ssh_username_host(&self) -> Result<SmallVec<[StackString; 4]>, Error> {
        let mut user_str = StackString::new();
        write!(user_str, "{}@{}", self.user, self.host)?;
        let mut port_str = StackString::new();
        write!(port_str, "{}", self.port)?;
        let ssh_str = if self.port == 22 {
            smallvec![
                "-C".into(),
                user_str,
            ]
        } else {
            smallvec![
                "-C".into(),
                "-p".into(),
                port_str,
                user_str,
            ]
        };

        Ok(ssh_str)
    }

    pub async fn run_command_stream_stdout(&self, cmd: &str) -> Result<StackString, Error> {
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock().await;
            info!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let mut args: SmallVec<[&str; 5]> = user_host.iter().map(StackString::as_str).collect();
            args.push(cmd);
            let process = Command::new("ssh").args(&args).output().await?;
            if !process.status.success() {
                error!("{}", StackString::from_utf8_lossy(&process.stderr));
                Err(format_err!("Process failed"))
            } else {
                StackString::from_utf8(process.stdout).map_err(Into::into)
            }
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub async fn run_command_print_stdout(&self, cmd: &str) -> Result<(), Error> {
        if let Some(host_lock) = LOCK_CACHE.read().await.get(&self.host) {
            let _guard = host_lock.lock();
            debug!("run_command_print_stdout cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let mut args: SmallVec<[&str; 4]> = user_host.iter().map(StackString::as_str).collect();
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
        let mut args: SmallVec<[&str; 4]> = user_host.iter().map(StackString::as_str).collect();
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
