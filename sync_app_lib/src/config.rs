use anyhow::{format_err, Error};
use std::env::var;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct ConfigInner {
    pub database_url: String,
    pub gdrive_secret_file: String,
    pub gdrive_token_path: String,
    pub aws_region_name: String,
    pub secret_key: String,
    pub domain: String,
    pub port: u32,
    pub n_db_workers: usize,
    pub garmin_username: Option<String>,
    pub garmin_password: Option<String>,
    pub garmin_from_url: Option<String>,
    pub garmin_to_url: Option<String>,
}

#[derive(Default, Debug, Clone)]
pub struct Config(Arc<ConfigInner>);

macro_rules! set_config_ok {
    ($s:ident, $id:ident) => {
        $s.$id = var(&stringify!($id).to_uppercase()).ok();
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
            .map_err(|e| format_err!("{} must be set: {}", stringify!($id).to_uppercase(), e))?;
    };
}

macro_rules! set_config_default {
    ($s:ident, $id:ident, $d:expr) => {
        $s.$id = var(&stringify!($id).to_uppercase()).unwrap_or_else(|_| $d);
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
            default_gdrive_secret.to_string_lossy().into()
        );
        set_config_default!(
            conf,
            gdrive_token_path,
            default_gdrive_token_path.to_string_lossy().into()
        );
        set_config_default!(conf, aws_region_name, "us-east-1".to_string());
        set_config_default!(conf, secret_key, "0123".repeat(8));
        set_config_default!(conf, domain, "localhost".to_string());
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
