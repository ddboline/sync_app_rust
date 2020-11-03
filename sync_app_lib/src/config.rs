use anyhow::{format_err, Error};
use derive_more::Into;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    env::var,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use url::Url;

use stack_string::StackString;

#[derive(Default, Debug, Deserialize)]
pub struct ConfigInner {
    pub database_url: StackString,
    #[serde(default = "default_gdrive_secret")]
    pub gdrive_secret_file: PathBuf,
    #[serde(default = "default_gdrive_token_path")]
    pub gdrive_token_path: PathBuf,
    #[serde(default = "default_aws_region_name")]
    pub aws_region_name: StackString,
    #[serde(default = "default_domain")]
    pub domain: StackString,
    #[serde(default = "default_port")]
    pub port: u32,
    #[serde(default = "default_n_db_workers")]
    pub n_db_workers: usize,
    pub remote_username: Option<StackString>,
    pub remote_password: Option<StackString>,
    pub remote_url: Option<UrlWrapper>,
    #[serde(default = "default_secret_path")]
    pub secret_path: PathBuf,
    #[serde(default = "default_secret_path")]
    pub jwt_secret_path: PathBuf,
}

#[derive(Default, Debug, Clone)]
pub struct Config(Arc<ConfigInner>);

fn home_dir() -> PathBuf {
    dirs::home_dir().expect("No HOME directory")
}
fn config_dir() -> PathBuf {
    dirs::config_dir().expect("No CONFIG directory")
}
fn default_gdrive_secret() -> PathBuf {
    config_dir()
        .join("sync_app_rust")
        .join("client_secrets.json")
}
fn default_gdrive_token_path() -> PathBuf {
    home_dir().join(".gdrive")
}
fn default_aws_region_name() -> StackString {
    "us-east-1".into()
}
fn default_port() -> u32 {
    3084
}
fn default_domain() -> StackString {
    "localhost".into()
}
fn default_n_db_workers() -> usize {
    2
}
fn default_secret_path() -> PathBuf {
    dirs::config_dir()
        .unwrap()
        .join("aws_app_rust")
        .join("secret.bin")
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init_config() -> Result<Self, Error> {
        let fname = Path::new("config.env");
        let config_dir = dirs::config_dir().ok_or_else(|| format_err!("No CONFIG directory"))?;
        let default_fname = config_dir.join("sync_app_rust").join("config.env");

        let env_file = if fname.exists() {
            fname
        } else {
            &default_fname
        };

        dotenv::dotenv().ok();

        if env_file.exists() {
            dotenv::from_path(env_file).ok();
        }

        let conf: ConfigInner = envy::from_env()?;

        Ok(Self(Arc::new(conf)))
    }
}

impl Deref for Config {
    type Target = ConfigInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Into, PartialEq)]
#[serde(into = "String", try_from = "String")]
pub struct UrlWrapper(Url);

impl From<UrlWrapper> for String {
    fn from(item: UrlWrapper) -> String {
        item.0.into_string()
    }
}

impl TryFrom<String> for UrlWrapper {
    type Error = Error;
    fn try_from(item: String) -> Result<Self, Self::Error> {
        let url: Url = item.parse()?;
        Ok(Self(url))
    }
}
