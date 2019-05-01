use std::env;
use std::path::Path;

pub struct Config {
    pub database_url: String,
}

impl Config {
    pub fn new() -> Config {
        let fname = "config.env";

        let home_dir = env::var("HOME").expect("No HOME directory...");

        let default_fname = format!("{}/.config/garmin_rust/config.env", home_dir);

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

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        Config { database_url }
    }
}
