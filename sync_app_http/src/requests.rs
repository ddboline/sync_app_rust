use lazy_static::lazy_static;
use rweb::Schema;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufRead, BufReader},
    path::Path,
    time::Instant,
};
use tokio::{process::Command, sync::Mutex, task::spawn_blocking};

use stack_string::StackString;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, file_sync::FileSyncAction,
    garmin_sync::GarminSync, models::FileSyncCache, movie_sync::MovieSync, pgpool::PgPool,
    sync_opts::SyncOpts,
};

use crate::{app::AccessLocks, errors::ServiceError as Error};

pub struct SyncRequest {
    pub action: FileSyncAction,
}

impl SyncRequest {
    pub async fn handle(
        &self,
        pool: &PgPool,
        config: &Config,
        locks: &AccessLocks,
    ) -> Result<Vec<StackString>, Error> {
        let _guard = locks.sync.lock().await;
        let opts = SyncOpts::new(self.action, &[]);
        opts.process_sync_opts(&config, pool)
            .await
            .map_err(Into::into)
    }
}

pub struct ListSyncCacheRequest {}

impl ListSyncCacheRequest {
    pub async fn handle(&self, pool: &PgPool) -> Result<Vec<FileSyncCache>, Error> {
        FileSyncCache::get_cache_list(pool)
            .await
            .map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Debug, Schema)]
pub struct SyncEntryDeleteRequest {
    pub id: i32,
}

impl SyncEntryDeleteRequest {
    pub async fn handle(&self, pool: &PgPool) -> Result<(), Error> {
        FileSyncCache::delete_by_id(pool, self.id)
            .await
            .map_err(Into::into)
    }
}

pub struct GarminSyncRequest {}

impl GarminSyncRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        locks
            .garmin
            .lock()
            .await
            .run_sync()
            .await
            .map_err(Into::into)
    }
}
pub struct MovieSyncRequest {}

impl MovieSyncRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        locks
            .movie
            .lock()
            .await
            .run_sync()
            .await
            .map_err(Into::into)
    }
}

pub struct CalendarSyncRequest {}

impl CalendarSyncRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        locks
            .calendar
            .lock()
            .await
            .run_sync()
            .await
            .map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Schema)]
pub struct SyncRemoveRequest {
    pub url: StackString,
}

impl SyncRemoveRequest {
    pub async fn handle(
        &self,
        locks: &AccessLocks,
        config: &Config,
        pool: &PgPool,
    ) -> Result<Vec<StackString>, Error> {
        let _guard = locks.sync.lock().await;
        let url = self.url.parse()?;
        let sync = SyncOpts::new(FileSyncAction::Delete, &[url]);
        sync.process_sync_opts(&config, pool)
            .await
            .map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize, Debug, Schema)]
pub struct SyncEntryProcessRequest {
    pub id: i32,
}

impl SyncEntryProcessRequest {
    pub async fn handle(
        &self,
        locks: &AccessLocks,
        pool: &PgPool,
        config: &Config,
    ) -> Result<(), Error> {
        let _guard = locks.sync.lock().await;

        let entry = FileSyncCache::get_by_id(pool, self.id).await?;
        let src_url = entry.src_url.parse()?;
        let dst_url = entry.dst_url.parse()?;
        let sync = SyncOpts::new(FileSyncAction::Copy, &[src_url, dst_url]);
        sync.process_sync_opts(&config, pool).await?;
        entry.delete_cache_entry(pool).await?;
        Ok(())
    }
}

pub struct SyncPodcastsRequest {}

impl SyncPodcastsRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        let _guard = locks.podcast.lock().await;
        if !Path::new("/usr/bin/podcatch-rust").exists() {
            return Ok(Vec::new());
        }
        let command = "/usr/bin/podcatch-rust".to_string();
        let process = Command::new(&command)
            .env_remove("DATABASE_URL")
            .env_remove("GOOGLE_MUSIC_DIRECTORY")
            .output()
            .await?;
        if process.stdout.is_empty() {
            Ok(Vec::new())
        } else {
            process
                .stdout
                .split(|c| *c == b'\n')
                .map(|s| {
                    String::from_utf8(s.to_vec())
                        .map_err(Into::into)
                        .map(Into::into)
                })
                .collect()
        }
    }
}

pub struct SyncSecurityRequest {}

impl SyncSecurityRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        let home_dir = match dirs::home_dir() {
            Some(home_dir) => home_dir,
            None => return Ok(Vec::new()),
        };
        let script = home_dir.join("bin").join("run_security_log_parse.sh");
        if !script.exists() {
            return Ok(Vec::new());
        }
        let _guard = locks.security.lock().await;
        let start_time = Instant::now();
        let lines: Result<Vec<StackString>, Error> = Command::new(&script)
            .env_remove("DATABASE_URL")
            .env_remove("EXPORT_DIR")
            .output()
            .await?
            .stdout
            .split(|c| *c == b'\n')
            .map(|s| {
                String::from_utf8(s.to_vec())
                    .map_err(Into::into)
                    .map(Into::into)
            })
            .collect();

        let run_time = Instant::now() - start_time;
        let mut lines = lines?;
        lines.push(format!("Run time {:0.2} s", run_time.as_secs_f64()).into());
        Ok(lines)
    }
}
