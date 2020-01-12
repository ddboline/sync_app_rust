use anyhow::{format_err, Error};
use lazy_static::lazy_static;
use log::debug;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::io::{stdout, Write};
use std::io::{BufRead, BufReader};
use subprocess::Exec;
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
    pub fn new(user: &str, host: &str, port: u16) -> Self {
        LOCK_CACHE.write().insert(host.to_string(), Mutex::new(()));
        Self {
            user: user.to_string(),
            host: host.to_string(),
            port,
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let host = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        Ok(Self::new(user, host, port))
    }

    pub fn get_ssh_str(&self, path: &str) -> Result<String, Error> {
        let ssh_str = if self.port == 22 {
            format!("{}@{}:{}", self.user, self.host, path)
        } else {
            format!("-p {} {}@{}:{}", self.port, self.user, self.host, path)
        };

        Ok(ssh_str)
    }

    pub fn get_ssh_username_host(&self) -> Result<String, Error> {
        let ssh_str = if self.port == 22 {
            format!("{}@{}", self.user, self.host)
        } else {
            format!("-p {} {}@{}", self.port, self.user, self.host)
        };

        Ok(ssh_str)
    }

    pub fn run_command_stream_stdout(&self, cmd: &str) -> Result<Vec<String>, Error> {
        if let Some(host_lock) = LOCK_CACHE.read().get(&self.host) {
            let _ = host_lock.lock();
            debug!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let command = format!(r#"ssh {} "{}""#, user_host, cmd);
            let stream = Exec::shell(command).stream_stdout()?;
            let reader = BufReader::new(stream);
            reader.lines().map(|line| Ok(line?)).collect()
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub fn run_command_print_stdout(&self, cmd: &str) -> Result<(), Error> {
        if let Some(host_lock) = LOCK_CACHE.read().get(&self.host) {
            let _ = host_lock.lock();
            debug!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let command = format!(r#"ssh {} "{}""#, user_host, cmd);
            let stream = Exec::shell(command).stream_stdout()?;
            let reader = BufReader::new(stream);

            let stdout = stdout();
            for line in reader.lines() {
                if let Ok(l) = line {
                    writeln!(stdout.lock(), "ssh://{}{}", user_host, l)?;
                }
            }
            Ok(())
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }

    pub fn run_command_ssh(&self, cmd: &str) -> Result<(), Error> {
        let user_host = self.get_ssh_username_host()?;
        let command = format!(r#"ssh {} "{}""#, user_host, cmd);
        self.run_command(&command)
    }

    pub fn run_command(&self, cmd: &str) -> Result<(), Error> {
        if let Some(host_lock) = LOCK_CACHE.read().get(&self.host) {
            let _ = host_lock.lock();
            debug!("cmd {}", cmd);
            if Exec::shell(cmd).join()?.success() {
                Ok(())
            } else {
                Err(format_err!("{} failed", cmd))
            }
        } else {
            Err(format_err!("Failed to acquire lock"))
        }
    }
}
