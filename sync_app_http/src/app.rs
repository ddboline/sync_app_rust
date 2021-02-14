use anyhow::Error;
use chrono::Duration;
use std::{net::SocketAddr, sync::Arc, time};
use tokio::{sync::Mutex, time::interval};
use warp::Filter;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, garmin_sync::GarminSync, movie_sync::MovieSync,
    pgpool::PgPool,
};

use super::{
    errors::error_response,
    logged_user::{fill_from_db, get_secrets, SECRET_KEY, TRIGGER_DB_UPDATE},
    routes::{
        delete_cache_entry, list_sync_cache, proc_all, process_cache_entry, remove, sync_all,
        sync_calendar, sync_frontpage, sync_garmin, sync_movie, sync_podcasts, sync_security, user,
    },
};

pub struct AccessLocks {
    pub sync: Mutex<()>,
    pub garmin: Mutex<GarminSync>,
    pub movie: Mutex<MovieSync>,
    pub calendar: Mutex<CalendarSync>,
    pub podcast: Mutex<()>,
    pub security: Mutex<()>,
}

impl AccessLocks {
    pub fn new(config: &Config) -> Self {
        Self {
            sync: Mutex::new(()),
            garmin: Mutex::new(GarminSync::new(config.clone())),
            movie: Mutex::new(MovieSync::new(config.clone())),
            calendar: Mutex::new(CalendarSync::new(config.clone())),
            podcast: Mutex::new(()),
            security: Mutex::new(()),
        }
    }
}

impl Default for AccessLocks {
    fn default() -> Self {
        Self::new(&Config::default())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: PgPool,
    pub locks: Arc<AccessLocks>,
}

pub async fn start_app() -> Result<(), Error> {
    async fn _update_db(pool: PgPool) {
        let mut i = interval(time::Duration::from_secs(60));
        loop {
            fill_from_db(&pool).await.unwrap_or(());
            i.tick().await;
        }
    }
    TRIGGER_DB_UPDATE.set();

    let config = Config::init_config().expect("Failed to load config");
    get_secrets(&config.secret_path, &config.jwt_secret_path).await?;
    let pool = PgPool::new(&config.database_url);

    tokio::task::spawn(_update_db(pool.clone()));

    run_app(config, pool).await
}

pub async fn run_app(config: Config, pool: PgPool) -> Result<(), Error> {
    let port = config.port;
    let locks = Arc::new(AccessLocks::new(&config));

    let data = AppState {
        config,
        db: pool,
        locks,
    };

    let data = warp::any().map(move || data.clone());

    let sync_frontpage_path = warp::path("index.html")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_frontpage)
        .boxed();
    let sync_all_path = warp::path("sync")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_all)
        .boxed();
    let proc_all_path = warp::path("proc_all")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(proc_all)
        .boxed();
    let process_cache_entry_path = warp::path("proc")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(process_cache_entry)
        .boxed();
    let remove_path = warp::path("remove")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(remove)
        .boxed();
    let list_sync_cache_path = warp::path("list_sync_cache")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(list_sync_cache)
        .boxed();
    let delete_cache_entry_path = warp::path("delete_cache_entry")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(delete_cache_entry)
        .boxed();
    let sync_garmin_path = warp::path("sync_garmin")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_garmin)
        .boxed();
    let sync_movie_path = warp::path("sync_movie")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_movie)
        .boxed();
    let sync_calendar_path = warp::path("sync_calendar")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_calendar)
        .boxed();
    let sync_podcasts_path = warp::path("sync_podcasts")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_podcasts)
        .boxed();
    let sync_security_path = warp::path("sync_security")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and(data.clone())
        .and_then(sync_security)
        .boxed();
    let user_path = warp::path("user")
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::cookie("jwt"))
        .and_then(user)
        .boxed();
    let sync_path = warp::path("sync")
        .and(
            sync_frontpage_path
                .or(sync_all_path)
                .or(proc_all_path)
                .or(process_cache_entry_path)
                .or(remove_path)
                .or(list_sync_cache_path)
                .or(delete_cache_entry_path)
                .or(sync_garmin_path)
                .or(sync_movie_path)
                .or(sync_calendar_path)
                .or(sync_podcasts_path)
                .or(sync_security_path)
                .or(user_path),
        )
        .boxed();

    let routes = sync_path.recover(error_response);
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    warp::serve(routes).bind(addr).await;
    Ok(())
}
