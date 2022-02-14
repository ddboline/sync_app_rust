use anyhow::{format_err, Error};
use chrono::Utc;
use futures::future::try_join_all;
use itertools::Itertools;
use log::{error, info};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use refinery::embed_migrations;
use stack_string::{format_sstr, StackString};
use std::{fmt::Write, sync::Arc};
use stdout_channel::StdoutChannel;
use structopt::StructOpt;
use tokio::task::spawn_blocking;
use url::Url;
use smallvec::SmallVec;

use crate::{
    calendar_sync::CalendarSync,
    config::Config,
    file_info::{FileInfo, FileInfoTrait},
    file_list::{group_urls, FileList, FileListTrait},
    file_service::FileService,
    file_sync::{FileSync, FileSyncAction},
    garmin_sync::GarminSync,
    models::{BlackList, FileSyncCache, FileSyncConfig},
    movie_sync::MovieSync,
    pgpool::PgPool,
    security_sync::SecuritySync,
};

embed_migrations!("../migrations");

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(parse(try_from_str))]
    /// Available commands are: "index", "sync", "proc(ess)", "copy" or "cp",
    /// "list" or "ls", "delete" or "rm", "move" or "mv", "ser" or
    /// "serialize", "add" or "add_config", "show", "show_cache"
    /// "sync_garmin", "sync_movie", "sync_calendar", "show_config",
    /// "sync_all", "run-migrations"
    pub action: FileSyncAction,
    #[structopt(short = "u", long = "urls", parse(try_from_str))]
    pub urls: Vec<Url>,
}

impl Default for SyncOpts {
    fn default() -> Self {
        Self {
            action: FileSyncAction::ShowCache,
            urls: Vec::new(),
        }
    }
}

impl SyncOpts {
    pub fn new(action: FileSyncAction, urls: &[Url]) -> Self {
        Self {
            action,
            urls: urls.into(),
        }
    }

    pub async fn process_args() -> Result<StdoutChannel<StackString>, Error> {
        let stdout = StdoutChannel::new();
        let opts = Self::from_args();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        if opts.action == FileSyncAction::SyncAll {
            for action in &[
                FileSyncAction::Sync,
                FileSyncAction::SyncGarmin,
                FileSyncAction::SyncMovie,
                FileSyncAction::SyncCalendar,
                FileSyncAction::SyncSecurity,
            ] {
                Self::new(*action, &[])
                    .process_sync_opts(&config, &pool, &stdout)
                    .await?;
            }
        } else {
            opts.process_sync_opts(&config, &pool, &stdout).await?;
        }
        Ok(stdout)
    }

    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::similar_names)]
    pub async fn process_sync_opts(
        &self,
        config: &Config,
        pool: &PgPool,
        stdout: &StdoutChannel<StackString>,
    ) -> Result<(), Error> {
        let blacklist = Arc::new(BlackList::new(pool).await.unwrap_or_default());
        match self.action {
            FileSyncAction::Index => {
                let _url_list: Vec<_>;
                let urls = if self.urls.is_empty() {
                    _url_list = FileSyncConfig::get_url_list(pool).await?;
                    &_url_list
                } else {
                    &self.urls
                };
                info!("urls: {:?}", urls);
                let futures = urls.iter().map(|url| {
                    let blacklist = Arc::clone(&blacklist);
                    let pool = pool.clone();
                    async move {
                        let mut flist = FileList::from_url(url, config, &pool).await?;
                        let list = flist.fill_file_list().await?;
                        let list: Vec<_> = if blacklist.could_be_in_blacklist(url) {
                            list.into_par_iter()
                                .filter(|entry| !blacklist.is_in_blacklist(&entry.urlname))
                                .collect()
                        } else {
                            list
                        };
                        flist.with_list(list);
                        flist.cache_file_list().await?;
                        Ok(flist)
                    }
                });
                let result: Result<Vec<_>, Error> = try_join_all(futures).await;
                result?;
                Ok(())
            }
            FileSyncAction::Sync => {
                let urls = if self.urls.is_empty() {
                    let futures = FileSyncCache::get_cache_list(pool)
                        .await?
                        .into_iter()
                        .map(|v| async move { v.delete_cache_entry(pool).await });
                    let result: Result<Vec<_>, Error> = try_join_all(futures).await;
                    result?;

                    FileSyncConfig::get_url_list(pool).await?
                } else {
                    self.urls.clone()
                };
                info!("Check 0");
                let futures = urls.into_iter().map(|url| {
                    let blacklist = blacklist.clone();
                    let pool = pool.clone();
                    async move {
                        let mut flist = FileList::from_url(&url, config, &pool).await?;
                        let list = flist.fill_file_list().await?;
                        let list: Vec<_> = if blacklist.could_be_in_blacklist(&url) {
                            list.into_iter()
                                .filter(|entry| !blacklist.is_in_blacklist(&entry.urlname))
                                .collect()
                        } else {
                            list
                        };
                        flist.with_list(list);
                        flist.cache_file_list().await?;
                        Ok(flist)
                    }
                });
                let flists: Result<Vec<_>, Error> = try_join_all(futures).await;
                let flists = flists?;
                info!("Check 1");
                let futures = flists.chunks(2).map(|f| async move {
                    if f.len() == 2 {
                        FileSync::compare_lists(&(*f[0]), &(*f[1]), pool).await?;
                    }
                    Ok(())
                });
                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                results?;
                info!("Check 2");
                for entry in FileSyncCache::get_cache_list(pool).await? {
                    let buf = format_sstr!("{} {}", entry.src_url, entry.dst_url);
                    stdout.send(buf);
                }
                Ok(())
            }
            FileSyncAction::Copy => {
                if self.urls.len() < 2 {
                    Err(format_err!("Need 2 Urls"))
                } else {
                    let finfo0 = FileInfo::from_url(&self.urls[0])?;
                    let finfo1 = FileInfo::from_url(&self.urls[1])?;

                    if finfo1.servicetype == FileService::Local {
                        let flist = FileList::from_url(&self.urls[0], config, pool).await?;
                        FileSync::copy_object(&(*flist), &finfo0, &finfo1).await?;
                    } else {
                        let flist = FileList::from_url(&self.urls[1], config, pool).await?;
                        FileSync::copy_object(&(*flist), &finfo0, &finfo1).await?;
                    }
                    Ok(())
                }
            }
            FileSyncAction::List => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    for urls in group_urls(&self.urls).values() {
                        let mut flist = FileList::from_url(&urls[0], config, pool).await?;
                        for url in urls {
                            flist.set_baseurl(url.clone());
                            flist.print_list(stdout).await?;
                        }
                    }
                    Ok(())
                }
            }
            FileSyncAction::Process => {
                let fsync = FileSync::new(config.clone());
                fsync.process_sync_cache(pool).await?;
                Ok(())
            }
            FileSyncAction::Delete => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let fsync = FileSync::new(config.clone());
                    fsync.delete_files(&self.urls, pool).await?;
                    Ok(())
                }
            }
            FileSyncAction::Move => {
                if self.urls.len() == 2 {
                    let finfo0 = FileInfo::from_url(&self.urls[0])?;
                    let finfo1 = FileInfo::from_url(&self.urls[1])?;

                    if finfo0.servicetype == finfo1.servicetype {
                        let flist = FileList::from_url(&self.urls[0], config, pool).await?;
                        flist.move_file(&finfo0, &finfo1).await?;
                        Ok(())
                    } else {
                        Err(format_err!("Can only move within servicetype"))
                    }
                } else {
                    Err(format_err!("Need 2 Urls"))
                }
            }
            FileSyncAction::Count => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let futures = self.urls.iter().map(|url| async move {
                        let pool = pool.clone();
                        let stdout = stdout.clone();
                        let mut flist = FileList::from_url(url, config, &pool).await?;
                        flist.with_list(flist.fill_file_list().await?);
                        let buf = format_sstr!("{}\t{}", url, flist.get_filemap().len());
                        stdout.send(buf);
                        Ok(())
                    });
                    let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                    results?;
                    Ok(())
                }
            }
            FileSyncAction::Serialize => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let futures = self.urls.iter().map(|url| async move {
                        let pool = pool.clone();
                        let stdout = stdout.clone();
                        let mut flist = FileList::from_url(url, config, &pool).await?;
                        flist.with_list(flist.fill_file_list().await?);
                        let results: Result<Vec<_>, Error> = flist
                            .get_filemap()
                            .values()
                            .map(|finfo| {
                                let line = serde_json::to_string(finfo.inner())?;
                                stdout.send(line);
                                Ok(())
                            })
                            .collect();
                        results
                    });
                    let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                    results?;
                    Ok(())
                }
            }
            FileSyncAction::AddConfig => {
                if self.urls.len() == 2 {
                    let conf = FileSyncConfig {
                        id: -1,
                        src_url: self.urls[0].as_str().into(),
                        dst_url: self.urls[1].as_str().into(),
                        last_run: Utc::now(),
                    };
                    conf.insert_config(pool).await?;
                    Ok(())
                } else {
                    Err(format_err!("Need exactly 2 Urls"))
                }
            }
            FileSyncAction::ShowConfig => {
                let clist = FileSyncConfig::get_config_list(pool)
                    .await?
                    .into_iter()
                    .map(|v| format_sstr!("{} {}", v.src_url, v.dst_url))
                    .join("\n");
                stdout.send(clist);
                Ok(())
            }
            FileSyncAction::ShowCache => {
                let clist = FileSyncCache::get_cache_list(pool)
                    .await?
                    .into_iter()
                    .map(|v| {
                        let buf = format_sstr!("{} {}", v.src_url, v.dst_url);
                        buf
                    })
                    .join("\n");
                stdout.send(clist);
                Ok(())
            }
            FileSyncAction::SyncGarmin => {
                let sync = GarminSync::new(config.clone());
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncMovie => {
                let sync = MovieSync::new(config.clone());
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncCalendar => {
                let sync = CalendarSync::new(config.clone());
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncSecurity => {
                let sync = SecuritySync::new(config.clone());
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncAll => Ok(()),
            FileSyncAction::RunMigrations => {
                let mut client = pool.get().await?;
                migrations::runner().run_async(&mut **client).await?;
                Ok(())
            }
        }
    }
}
