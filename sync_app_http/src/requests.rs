use anyhow::Error;
use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader};
use std::path::Path;
use subprocess::Exec;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, file_sync::FileSyncAction,
    garmin_sync::GarminSync, models::FileSyncCache, movie_sync::MovieSync, pgpool::PgPool,
    sync_opts::SyncOpts,
};

lazy_static! {
    static ref CONFIG: Config = Config::init_config().expect("Failed to load config");
    static ref SYNCLOCK: Mutex<()> = Mutex::new(());
    static ref GARMINLOCK: Mutex<()> = Mutex::new(());
    static ref MOVIELOCK: Mutex<()> = Mutex::new(());
    static ref CALENDARLOCK: Mutex<()> = Mutex::new(());
    static ref PODCASTLOCK: Mutex<()> = Mutex::new(());
}

#[async_trait]
pub trait HandleRequest<T> {
    type Result;
    async fn handle(&self, req: T) -> Self::Result;
}

pub struct SyncRequest {
    pub action: FileSyncAction,
}

#[async_trait]
impl HandleRequest<SyncRequest> for PgPool {
    type Result = Result<(), Error>;
    async fn handle(&self, req: SyncRequest) -> Self::Result {
        let _ = SYNCLOCK.lock().await;
        let opts = SyncOpts::new(req.action, &[]);
        opts.process_sync_opts(&CONFIG, self).await
    }
}

pub struct ListSyncCacheRequest {}

#[async_trait]
impl HandleRequest<ListSyncCacheRequest> for PgPool {
    type Result = Result<Vec<FileSyncCache>, Error>;
    async fn handle(&self, _: ListSyncCacheRequest) -> Self::Result {
        FileSyncCache::get_cache_list(self).await
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncEntryDeleteRequest {
    pub id: i32,
}

#[async_trait]
impl HandleRequest<SyncEntryDeleteRequest> for PgPool {
    type Result = Result<(), Error>;
    async fn handle(&self, req: SyncEntryDeleteRequest) -> Self::Result {
        FileSyncCache::delete_by_id(self, req.id).await
    }
}

pub struct GarminSyncRequest {}

#[async_trait]
impl HandleRequest<GarminSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    async fn handle(&self, _: GarminSyncRequest) -> Self::Result {
        let _ = GARMINLOCK.lock().await;
        let config = CONFIG.clone();
        let sync = GarminSync::new(config);
        sync.run_sync().await
    }
}
pub struct MovieSyncRequest {}

#[async_trait]
impl HandleRequest<MovieSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    async fn handle(&self, _: MovieSyncRequest) -> Self::Result {
        let _ = MOVIELOCK.lock().await;
        let config = CONFIG.clone();
        let sync = MovieSync::new(config);
        sync.run_sync().await
    }
}

pub struct CalendarSyncRequest {}

#[async_trait]
impl HandleRequest<CalendarSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    async fn handle(&self, _: CalendarSyncRequest) -> Self::Result {
        let _ = CALENDARLOCK.lock().await;
        let config = CONFIG.clone();
        let sync = CalendarSync::new(config);
        sync.run_sync().await
    }
}

#[derive(Serialize, Deserialize)]
pub struct SyncRemoveRequest {
    pub url: String,
}

#[async_trait]
impl HandleRequest<SyncRemoveRequest> for PgPool {
    type Result = Result<(), Error>;
    async fn handle(&self, req: SyncRemoveRequest) -> Self::Result {
        let url = req.url.parse()?;
        let sync = SyncOpts::new(FileSyncAction::Delete, &[url]);
        sync.process_sync_opts(&CONFIG, self).await
    }
}

pub struct SyncPodcastsRequest {}

#[async_trait]
impl HandleRequest<SyncPodcastsRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    async fn handle(&self, _: SyncPodcastsRequest) -> Self::Result {
        let _ = PODCASTLOCK.lock().await;
        if !Path::new("/usr/bin/podcatch-rust").exists() {
            return Ok(Vec::new());
        }
        let command = "/usr/bin/podcatch-rust -g".to_string();
        spawn_blocking(move || {
            let stream = Exec::shell(command)
                .env_remove("DATABASE_URL")
                .env_remove("GOOGLE_MUSIC_DIRECTORY")
                .stream_stdout()?;
            let reader = BufReader::new(stream);
            reader.lines().map(|line| Ok(line?)).collect()
        })
        .await?
    }
}
