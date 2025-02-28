use anyhow::Error;
use deadqueue::unlimited::Queue;
use log::error;
use reqwest::{Client, ClientBuilder};
use rweb::{
    filters::BoxedFilter,
    http::header::CONTENT_TYPE,
    openapi::{self, Info},
    Filter, Reply,
};
use stack_string::format_sstr;
use std::{net::SocketAddr, sync::Arc, time};
use tokio::{sync::Mutex, task::JoinHandle, time::interval};

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, garmin_sync::GarminSync, movie_sync::MovieSync,
    pgpool::PgPool, security_sync::SecuritySync, sync_opts::SyncOpts, weather_sync::WeatherSync,
};

use super::{
    errors::error_response,
    logged_user::{fill_from_db, get_secrets, SyncMesg},
    routes::{
        delete_cache_entry, garmin_scripts_js, list_sync_cache, proc_all, process_cache_entry,
        remove, sync_all, sync_calendar, sync_frontpage, sync_garmin, sync_movie, sync_name,
        sync_podcasts, sync_security, sync_weather, user,
    },
};

pub struct AccessLocks {
    pub sync: Mutex<SyncOpts>,
    pub garmin: Mutex<GarminSync>,
    pub movie: Mutex<MovieSync>,
    pub calendar: Mutex<CalendarSync>,
    pub podcast: Mutex<()>,
    pub security: Mutex<SecuritySync>,
    pub weather: Mutex<WeatherSync>,
}

impl AccessLocks {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: &Config) -> Result<Self, Error> {
        Ok(Self {
            sync: Mutex::new(SyncOpts::default()),
            garmin: Mutex::new(GarminSync::new(config.clone())?),
            movie: Mutex::new(MovieSync::new(config.clone())?),
            calendar: Mutex::new(CalendarSync::new(config.clone())?),
            podcast: Mutex::new(()),
            security: Mutex::new(SecuritySync::new(config.clone())?),
            weather: Mutex::new(WeatherSync::new(config.clone())?),
        })
    }
}

type SyncJob = (SyncMesg, JoinHandle<Result<(), Error>>);

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: PgPool,
    pub locks: Arc<AccessLocks>,
    pub client: Arc<Client>,
    pub queue: Arc<Queue<SyncJob>>,
}

/// # Errors
/// Return error if app init fails
pub async fn start_app() -> Result<(), Error> {
    async fn update_db(pool: PgPool) {
        let mut i = interval(time::Duration::from_secs(60));
        loop {
            fill_from_db(&pool).await.unwrap_or(());
            i.tick().await;
        }
    }

    let config = Config::init_config()?;
    get_secrets(&config.secret_path, &config.jwt_secret_path).await?;
    let pool = PgPool::new(&config.database_url)?;

    tokio::task::spawn(update_db(pool.clone()));

    run_app(config, pool).await
}

fn get_sync_path(app: &AppState) -> BoxedFilter<(impl Reply,)> {
    let sync_frontpage_path = sync_frontpage(app.clone()).boxed();
    let garmin_scripts_js_path = garmin_scripts_js().boxed();
    let sync_all_path = sync_all(app.clone()).boxed();
    let sync_name_path = sync_name(app.clone()).boxed();
    let proc_all_path = proc_all(app.clone()).boxed();
    let process_cache_entry_path = process_cache_entry(app.clone()).boxed();
    let remove_path = remove(app.clone()).boxed();
    let list_sync_cache_path = list_sync_cache(app.clone()).boxed();
    let delete_cache_entry_path = delete_cache_entry(app.clone()).boxed();
    let sync_garmin_path = sync_garmin(app.clone()).boxed();
    let sync_movie_path = sync_movie(app.clone()).boxed();
    let sync_calendar_path = sync_calendar(app.clone()).boxed();
    let sync_podcasts_path = sync_podcasts(app.clone()).boxed();
    let sync_security_path = sync_security(app.clone()).boxed();
    let sync_weather_path = sync_weather(app.clone()).boxed();
    let user_path = user().boxed();
    sync_frontpage_path
        .or(garmin_scripts_js_path)
        .or(sync_all_path)
        .or(sync_name_path)
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
        .or(sync_weather_path)
        .or(user_path)
        .boxed()
}

async fn run_app(config: Config, pool: PgPool) -> Result<(), Error> {
    async fn run_queue(app: AppState) {
        loop {
            let (SyncMesg { user, key }, task) = app.queue.pop().await;
            match task.await {
                Ok(Err(e)) => {
                    error!("Failure running job {}", e);
                    if let Err(e) = user
                        .rm_session(&app.client, &app.config, key.to_str())
                        .await
                    {
                        error!("Failed to delete session {}", e);
                    }
                }
                Err(e) => error!("join error {}", e),
                _ => {}
            }
        }
    }

    let port = config.port;
    let locks = Arc::new(AccessLocks::new(&config)?);
    let client = Arc::new(ClientBuilder::new().build()?);
    let queue = Arc::new(Queue::new());

    let app = AppState {
        config,
        db: pool,
        locks,
        client,
        queue,
    };

    tokio::task::spawn(run_queue(app.clone()));

    let (spec, sync_path) = openapi::spec()
        .info(Info {
            title: "File Sync WebApp".into(),
            description: "Web Frontend for File Sync Service".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            ..Info::default()
        })
        .build(|| get_sync_path(&app));
    let spec = Arc::new(spec);
    let spec_json_path = rweb::path!("sync" / "openapi" / "json")
        .and(rweb::path::end())
        .map({
            let spec = spec.clone();
            move || rweb::reply::json(spec.as_ref())
        });

    let spec_yaml = serde_yaml::to_string(spec.as_ref())?;
    let spec_yaml_path = rweb::path!("sync" / "openapi" / "yaml")
        .and(rweb::path::end())
        .map(move || {
            let reply = rweb::reply::html(spec_yaml.clone());
            rweb::reply::with_header(reply, CONTENT_TYPE, "text/yaml")
        });

    let routes = sync_path
        .or(spec_json_path)
        .or(spec_yaml_path)
        .recover(error_response);
    let addr: SocketAddr = format_sstr!("127.0.0.1:{port}").parse()?;
    rweb::serve(routes).bind(addr).await;
    Ok(())
}
