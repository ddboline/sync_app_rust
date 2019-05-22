use std::env::var;
use std::path::Path;

#[derive(Default, Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub gdrive_secret_file: String,
    pub gdrive_token_path: String,
    pub aws_region_name: String,
}

impl Config {
    pub fn new() -> Config {
        let fname = "config.env";

        let home_dir = var("HOME").expect("No HOME directory...");

        let default_fname = format!("{}/.config/sync_app_rust/config.env", home_dir);

        let env_file = if Path::new(fname).exists() {
            fname.to_string()
        } else {
            default_fname
        };

        if Path::new(&env_file).exists() {
            dotenv::from_path(&env_file).ok();
        } else if Path::new("config.env").exists() {
            dotenv::from_filename("config.env").ok();
        } else {
            dotenv::dotenv().ok();
        }

        let database_url = var("DATABASE_URL").expect("DATABASE_URL must be set");
        let gdrive_secret_file = var("GDRIVE_SECRET_FILE")
            .unwrap_or_else(|_| format!("{}/.config/sync_app_rust/client_secrets.json", home_dir));
        let gdrive_token_path =
            var("GDRIVE_TOKEN_PATH").unwrap_or_else(|_| format!("{}/.gdrive", home_dir));
        let aws_region_name = var("AWS_REGION_NAME").unwrap_or_else(|_| "us-east-1".to_string());

        Config {
            database_url,
            gdrive_secret_file,
            gdrive_token_path,
            aws_region_name,
        }
    }
}
