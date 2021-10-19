use anyhow::Error;
use chrono::Duration;
use deadqueue::unlimited::Queue;
use log::{debug, error};
use reqwest::{Client, ClientBuilder};
use rweb::{
    filters::BoxedFilter,
    http::header::CONTENT_TYPE,
    openapi::{self, Info},
    Filter, Reply,
};
use stack_string::StackString;
use std::{net::SocketAddr, sync::Arc, time};
use tokio::{sync::Mutex, time::interval};
use uuid::Uuid;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, file_sync::FileSyncAction,
    garmin_sync::GarminSync, movie_sync::MovieSync, pgpool::PgPool,
};

use crate::logged_user::{LoggedUser, SyncKey, SyncSession};

use super::{
    errors::error_response,
    logged_user::{fill_from_db, get_secrets, SECRET_KEY, TRIGGER_DB_UPDATE},
    requests::{
        CalendarSyncRequest, GarminSyncRequest, MovieSyncRequest, SyncPodcastsRequest, SyncRequest,
        SyncSecurityRequest,
    },
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

pub struct SyncMesg {
    pub user: LoggedUser,
    pub key: SyncKey,
}

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: PgPool,
    pub locks: Arc<AccessLocks>,
    pub client: Arc<Client>,
    pub queue: Arc<Queue<SyncMesg>>,
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

fn get_sync_path(app: &AppState) -> BoxedFilter<(impl Reply,)> {
    let sync_frontpage_path = sync_frontpage(app.clone()).boxed();
    let sync_all_path = sync_all(app.clone()).boxed();
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
    let user_path = user().boxed();
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
        .or(user_path)
        .boxed()
}

pub async fn run_app(config: Config, pool: PgPool) -> Result<(), Error> {
    async fn _run_queue(app: AppState) {
        loop {
            let SyncMesg { user, key } = app.queue.pop().await;
            debug!("start {} for {} {}", key.to_str(), user.email, user.session);
            let result = match key {
                SyncKey::SyncGarmin => (GarminSyncRequest {}).handle(&app.locks).await,
                SyncKey::SyncMovie => (MovieSyncRequest {}).handle(&app.locks).await,
                SyncKey::SyncCalendar => (CalendarSyncRequest {}).handle(&app.locks).await,
                SyncKey::SyncPodcast => (SyncPodcastsRequest {}).handle(&app.locks).await,
                SyncKey::SyncSecurity => (SyncSecurityRequest {}).handle(&app.locks).await,
            };
            let lines = match result {
                Ok(lines) => lines,
                Err(e) => {
                    error!("Failed to run sync job {:?}", e);
                    if let Err(e) = user
                        .rm_session(&app.client, &app.config, key.to_str())
                        .await
                    {
                        error!("Failed to delete session {:?}", e);
                    }
                    continue;
                }
            };
            debug!(
                "finished {} for {} {}, {} lines",
                key.to_str(),
                user.email,
                user.session,
                lines.len()
            );
            let value = SyncSession::from_lines(lines);
            if let Err(e) = user
                .set_session(&app.client, &app.config, key.to_str(), value)
                .await
            {
                error!("Failed to set session {:?}", e);
            }
        }
    }

    let port = config.port;
    let locks = Arc::new(AccessLocks::new(&config));
    let client = Arc::new(ClientBuilder::new().build()?);
    let queue = Arc::new(Queue::new());

    let app = AppState {
        config,
        db: pool,
        locks,
        client,
        queue,
    };

    tokio::task::spawn(_run_queue(app.clone()));

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
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    rweb::serve(routes).bind(addr).await;
    Ok(())
}
