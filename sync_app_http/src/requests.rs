use lazy_static::lazy_static;
use log::debug;
use rweb::Schema;
use serde::{Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{
    fmt::Write,
    io::{BufRead, BufReader},
    path::Path,
    time::Instant,
};
use stdout_channel::{MockStdout, StdoutChannel};
use tokio::{process::Command, sync::Mutex, task::spawn_blocking};

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
        let mut sync = locks.sync.lock().await;
        sync.action = self.action;
        sync.urls = Vec::new();
        let mock_stdout = MockStdout::new();
        let stdout = StdoutChannel::with_mock_stdout(mock_stdout.clone(), mock_stdout.clone());
        sync.process_sync_opts(config, pool, &stdout).await?;
        stdout.close().await?;
        let mut output = Vec::new();
        while let Some(line) = mock_stdout.lock().await.pop() {
            output.push(line);
        }
        output.reverse();
        Ok(output)
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
        let mut sync = locks.sync.lock().await;
        let url = self.url.parse()?;
        sync.action = FileSyncAction::Delete;
        sync.urls = vec![url];
        let mock_stdout = MockStdout::new();
        let stdout = StdoutChannel::with_mock_stdout(mock_stdout.clone(), mock_stdout.clone());
        sync.process_sync_opts(config, pool, &stdout).await?;
        stdout.close().await?;
        let mut output = Vec::new();
        while let Some(line) = mock_stdout.lock().await.pop() {
            output.push(line);
        }
        output.reverse();
        Ok(output)
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
        let mut sync = locks.sync.lock().await;
        let entry = FileSyncCache::get_by_id(pool, self.id)
            .await?
            .ok_or_else(|| Error::BadRequest("No entry".into()))?;
        let src_url = entry.src_url.parse()?;
        let dst_url = entry.dst_url.parse()?;
        sync.action = FileSyncAction::Copy;
        sync.urls = vec![src_url, dst_url];
        let mock_stdout = MockStdout::new();
        let stdout = StdoutChannel::with_mock_stdout(mock_stdout.clone(), mock_stdout.clone());
        sync.process_sync_opts(config, pool, &stdout).await?;
        stdout.close().await?;
        debug!("{}", mock_stdout.lock().await.join("\n"));
        entry.delete_cache_entry(pool).await?;
        Ok(())
    }
}

pub struct SyncPodcastsRequest {}

impl SyncPodcastsRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        let _guard = locks.podcast.lock().await;
        let command_path = "/usr/bin/podcatch-rust";
        if !Path::new(command_path).exists() {
            return Ok(Vec::new());
        }
        let process = Command::new(&command_path)
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
                    let mut buf = StackString::new();
                    let s = std::str::from_utf8(s)?;
                    buf.push_str(s);
                    Ok(buf)
                })
                .collect()
        }
    }
}

pub struct SyncSecurityRequest {}

impl SyncSecurityRequest {
    pub async fn handle(&self, locks: &AccessLocks) -> Result<Vec<StackString>, Error> {
        locks
            .security
            .lock()
            .await
            .run_sync()
            .await
            .map_err(Into::into)
    }
}
