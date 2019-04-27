use diesel::pg::PgConnection;
use diesel::prelude::*;
use std::env;
use std::path::Path;

fn main() {
    println!("Hello, world!");
}

pub fn establish_connection() -> PgConnection {
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
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}
