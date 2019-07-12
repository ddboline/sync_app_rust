use failure::{err_msg, Error};
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
}

#[derive(Default, Debug, Clone)]
pub struct Config(Arc<ConfigInner>);

impl Config {
    pub fn new() -> Config {
        Default::default()
    }

    pub fn init_config() -> Result<Config, Error> {
        let fname = "config.env";

        let home_dir = var("HOME").map_err(|e| err_msg(format!("No HOME directory {}", e)))?;

        let default_fname = format!("{}/.config/sync_app_rust/config.env", home_dir);

        let env_file = if Path::new(fname).exists() {
            fname.to_string()
        } else {
            default_fname
        };

        dotenv::dotenv().ok();

        if Path::new(&env_file).exists() {
            dotenv::from_path(&env_file).ok();
        } else if Path::new("config.env").exists() {
            dotenv::from_filename("config.env").ok();
        }

        let default_gdrive_secret =
            format!("{}/.config/sync_app_rust/client_secrets.json", home_dir);
        let default_gdrive_token_path = format!("{}/.gdrive", home_dir);

        let conf = ConfigInner {
            database_url: var("DATABASE_URL")
                .map_err(|e| err_msg(format!("DATABASE_URL must be set {}", e)))?,
            gdrive_secret_file: var("GDRIVE_SECRET_FILE").unwrap_or_else(|_| default_gdrive_secret),
            gdrive_token_path: var("GDRIVE_TOKEN_PATH")
                .unwrap_or_else(|_| default_gdrive_token_path),
            aws_region_name: var("AWS_REGION_NAME").unwrap_or_else(|_| "us-east-1".to_string()),
        };

        Ok(Config(Arc::new(conf)))
    }
}

impl Deref for Config {
    type Target = ConfigInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
