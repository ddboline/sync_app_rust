use anyhow::{format_err, Error};
use std::{env::var, ops::Deref, path::Path, sync::Arc};

use crate::stack_string::StackString;

#[derive(Default, Debug)]
pub struct ConfigInner {
    pub database_url: StackString,
    pub gdrive_secret_file: StackString,
    pub gdrive_token_path: StackString,
    pub aws_region_name: StackString,
    pub secret_key: StackString,
    pub domain: StackString,
    pub port: u32,
    pub n_db_workers: usize,
    pub garmin_username: Option<StackString>,
    pub garmin_password: Option<StackString>,
    pub garmin_from_url: Option<StackString>,
    pub garmin_to_url: Option<StackString>,
}

#[derive(Default, Debug, Clone)]
pub struct Config(Arc<ConfigInner>);

macro_rules! set_config_ok {
    ($s:ident, $id:ident) => {
        $s.$id = var(&stringify!($id).to_uppercase()).ok().map(Into::into);
    };
}

macro_rules! set_config_parse {
    ($s:ident, $id:ident, $d:expr) => {
        $s.$id = var(&stringify!($id).to_uppercase())
            .ok()
            .and_then(|x| x.parse().ok())
            .unwrap_or_else(|| $d);
    };
}

macro_rules! set_config_must {
    ($s:ident, $id:ident) => {
        $s.$id = var(&stringify!($id).to_uppercase())
            .map(Into::into)
            .map_err(|e| format_err!("{} must be set: {}", stringify!($id).to_uppercase(), e))?;
    };
}

macro_rules! set_config_default {
    ($s:ident, $id:ident, $d:expr) => {
        $s.$id = var(&stringify!($id).to_uppercase()).map_or_else(|_| $d, Into::into);
    };
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init_config() -> Result<Self, Error> {
        let fname = Path::new("config.env");
        let config_dir = dirs::config_dir().ok_or_else(|| format_err!("No CONFIG directory"))?;
        let home_dir = dirs::home_dir().ok_or_else(|| format_err!("No HOME directory"))?;
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

        let default_gdrive_secret = config_dir.join("sync_app_rust").join("client_secrets.json");
        let default_gdrive_token_path = home_dir.join(".gdrive");

        let mut conf = ConfigInner::default();

        set_config_must!(conf, database_url);
        set_config_default!(
            conf,
            gdrive_secret_file,
            default_gdrive_secret.to_string_lossy().as_ref().into()
        );
        set_config_default!(
            conf,
            gdrive_token_path,
            default_gdrive_token_path.to_string_lossy().as_ref().into()
        );
        set_config_default!(conf, aws_region_name, "us-east-1".into());
        set_config_default!(conf, secret_key, "0123".repeat(8).into());
        set_config_default!(conf, domain, "localhost".into());
        set_config_parse!(conf, port, 3084);
        set_config_parse!(conf, n_db_workers, 2);
        set_config_ok!(conf, garmin_username);
        set_config_ok!(conf, garmin_password);
        set_config_ok!(conf, garmin_from_url);
        set_config_ok!(conf, garmin_to_url);

        Ok(Self(Arc::new(conf)))
    }
}

impl Deref for Config {
    type Target = ConfigInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
