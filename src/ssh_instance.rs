use failure::{err_msg, Error};
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};
use subprocess::Exec;
use url::Url;

use crate::map_result;

#[derive(Debug, Clone)]
pub struct SSHInstance {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub ssh_lock: Arc<Mutex<()>>,
}

impl SSHInstance {
    pub fn new(user: &str, host: &str, port: u16) -> Self {
        Self {
            user: user.to_string(),
            host: host.to_string(),
            port: port,
            ssh_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        let host = url.host_str().ok_or_else(|| err_msg("Parse error"))?;
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
        if let Ok(_) = self.ssh_lock.lock() {
            println!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let command = format!(r#"ssh {} "{}""#, user_host, cmd);
            let stream = Exec::shell(command).stream_stdout()?;
            let reader = BufReader::new(stream);

            let results: Vec<_> = reader.lines().map(|line| Ok(line?)).collect();
            let results: Vec<String> = map_result(results)?;
            Ok(results)
        } else {
            Err(err_msg("Failed to acquire lock"))
        }
    }

    pub fn run_command_print_stdout(&self, cmd: &str) -> Result<(), Error> {
        if let Ok(_) = self.ssh_lock.lock() {
            println!("cmd {}", cmd);
            let user_host = self.get_ssh_username_host()?;
            let command = format!(r#"ssh {} "{}""#, user_host, cmd);
            let stream = Exec::shell(command).stream_stdout()?;
            let reader = BufReader::new(stream);

            for line in reader.lines() {
                if let Ok(l) = line {
                    println!("ssh://{}{}", user_host, l);
                }
            }
            Ok(())
        } else {
            Err(err_msg("Failed to acquire lock"))
        }
    }

    pub fn run_command_ssh(&self, cmd: &str) -> Result<(), Error> {
        let user_host = self.get_ssh_username_host()?;
        let command = format!(r#"ssh {} "{}""#, user_host, cmd);
        self.run_command(&command)
    }

    pub fn run_command(&self, cmd: &str) -> Result<(), Error> {
        if let Ok(_) = self.ssh_lock.lock() {
            println!("cmd {}", cmd);
            let status = Exec::shell(cmd).join()?;
            if !status.success() {
                Err(err_msg(format!("{} failed", cmd)))
            } else {
                Ok(())
            }
        } else {
            Err(err_msg("Failed to acquire lock"))
        }
    }
}
