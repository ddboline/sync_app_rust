use anyhow::Error;
use async_trait::async_trait;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    time::Instant,
};
use subprocess::Exec;
use tokio::{sync::Mutex, task::spawn_blocking};

use stack_string::StackString;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, file_sync::FileSyncAction,
    garmin_sync::GarminSync, models::FileSyncCache, movie_sync::MovieSync, pgpool::PgPool,
    sync_opts::SyncOpts,
};

lazy_static! {
    static ref CONFIG: Config = Config::init_config().expect("Failed to load config");
    static ref SYNCLOCK: Mutex<()> = Mutex::new(());
    static ref GARMINLOCK: Mutex<GarminSync> = Mutex::new(GarminSync::new(CONFIG.clone()));
    static ref MOVIELOCK: Mutex<MovieSync> = Mutex::new(MovieSync::new(CONFIG.clone()));
    static ref CALENDARLOCK: Mutex<CalendarSync> = Mutex::new(CalendarSync::new(CONFIG.clone()));
    static ref PODCASTLOCK: Mutex<()> = Mutex::new(());
    static ref SECURITYLOCK: Mutex<()> = Mutex::new(());
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
    type Result = Result<Vec<StackString>, Error>;
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
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, _: GarminSyncRequest) -> Self::Result {
        GARMINLOCK.lock().await.run_sync().await
    }
}
pub struct MovieSyncRequest {}

#[async_trait]
impl HandleRequest<MovieSyncRequest> for PgPool {
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, _: MovieSyncRequest) -> Self::Result {
        MOVIELOCK.lock().await.run_sync().await
    }
}

pub struct CalendarSyncRequest {}

#[async_trait]
impl HandleRequest<CalendarSyncRequest> for PgPool {
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, _: CalendarSyncRequest) -> Self::Result {
        CALENDARLOCK.lock().await.run_sync().await
    }
}

#[derive(Serialize, Deserialize)]
pub struct SyncRemoveRequest {
    pub url: StackString,
}

#[async_trait]
impl HandleRequest<SyncRemoveRequest> for PgPool {
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, req: SyncRemoveRequest) -> Self::Result {
        let _ = SYNCLOCK.lock().await;
        let url = req.url.parse()?;
        let sync = SyncOpts::new(FileSyncAction::Delete, &[url]);
        sync.process_sync_opts(&CONFIG, self).await
    }
}

pub struct SyncPodcastsRequest {}

#[async_trait]
impl HandleRequest<SyncPodcastsRequest> for PgPool {
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, _: SyncPodcastsRequest) -> Self::Result {
        let _ = PODCASTLOCK.lock().await;
        if !Path::new("/usr/bin/podcatch-rust").exists() {
            return Ok(Vec::new());
        }
        let command = "/usr/bin/podcatch-rust".to_string();
        spawn_blocking(move || {
            let stream = Exec::shell(command)
                .env_remove("DATABASE_URL")
                .env_remove("GOOGLE_MUSIC_DIRECTORY")
                .stream_stdout()?;
            let reader = BufReader::new(stream);
            reader.lines().map(|line| Ok(line?.into())).collect()
        })
        .await?
    }
}

pub struct SyncSecurityRequest {}

#[async_trait]
impl HandleRequest<SyncSecurityRequest> for PgPool {
    type Result = Result<Vec<StackString>, Error>;
    async fn handle(&self, _: SyncSecurityRequest) -> Self::Result {
        let home_dir = match dirs::home_dir() {
            Some(home_dir) => home_dir,
            _ => return Ok(Vec::new()),
        };
        let script = home_dir.join("bin").join("run_security_log_parse.sh");
        if !script.exists() {
            return Ok(Vec::new());
        }
        let _ = SECURITYLOCK.lock().await;
        let start_time = Instant::now();
        let lines: Result<Vec<StackString>, Error> = spawn_blocking(move || {
            let stream = Exec::cmd(script)
                .env_remove("DATABASE_URL")
                .env_remove("EXPORT_DIR")
                .stream_stdout()?;
            let reader = BufReader::new(stream);
            reader.lines().map(|line| Ok(line?.into())).collect()
        })
        .await?;
        let run_time = Instant::now() - start_time;
        let mut lines = lines?;
        lines.push(format!("Run time {:0.2} s", run_time.as_secs_f64()).into());
        Ok(lines)
    }
}
