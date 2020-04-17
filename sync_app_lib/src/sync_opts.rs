use anyhow::{format_err, Error};
use futures::future::try_join_all;
use itertools::Itertools;
use log::debug;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use std::sync::Arc;
use structopt::StructOpt;
use url::Url;

use crate::{
    calendar_sync::CalendarSync,
    config::Config,
    file_info::{FileInfo, FileInfoTrait},
    file_list::{group_urls, FileList, FileListTrait},
    file_service::FileService,
    file_sync::{FileSync, FileSyncAction},
    garmin_sync::GarminSync,
    models::{BlackList, FileSyncCache, FileSyncConfig, InsertFileSyncConfig},
    movie_sync::MovieSync,
    pgpool::PgPool,
};

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(parse(try_from_str))]
    /// Available commands are: "index", "sync", "proc(ess)", "copy" or "cp",
    /// "list" or "ls", "delete" or "rm", "move" or "mv", "ser" or
    /// "serialize", "add" or "add_config", "show", "show_cache"
    /// "sync_garmin", "sync_movie", "sync_calendar"
    pub action: FileSyncAction,
    #[structopt(short = "u", long = "urls", parse(try_from_str))]
    pub urls: Vec<Url>,
}

impl SyncOpts {
    pub fn new(action: FileSyncAction, urls: &[Url]) -> Self {
        Self {
            action,
            urls: urls.to_vec(),
        }
    }

    pub async fn process_args() -> Result<Vec<String>, Error> {
        let opts = Self::from_args();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        opts.process_sync_opts(&config, &pool).await
    }

    #[allow(clippy::cognitive_complexity)]
    pub async fn process_sync_opts(
        &self,
        config: &Config,
        pool: &PgPool,
    ) -> Result<Vec<String>, Error> {
        let mut output = Vec::new();
        let blacklist = Arc::new(BlackList::new(pool).await?);

        match self.action {
            FileSyncAction::Index => {
                let urls = if self.urls.is_empty() {
                    FileSyncConfig::get_url_list(&pool).await?
                } else {
                    self.urls.to_vec()
                };
                debug!("urls: {:?}", urls);
                let futures = urls.into_iter().map(|url| {
                    let blacklist = Arc::clone(&blacklist);
                    let pool = pool.clone();
                    async move {
                        let mut flist = FileList::from_url(&url, &config, &pool).await?;
                        let list = flist.fill_file_list().await?;
                        let list: Vec<_> = if blacklist.could_be_in_blacklist(&url) {
                            list.into_par_iter()
                                .filter(|entry| {
                                    if let Some(url) = entry.urlname.as_ref() {
                                        !blacklist.is_in_blacklist(url)
                                    } else {
                                        true
                                    }
                                })
                                .collect()
                        } else {
                            list
                        };
                        flist.with_list(list);
                        flist.cache_file_list()?;
                        Ok(flist)
                    }
                });
                let result: Result<Vec<_>, Error> = try_join_all(futures).await;
                result?;
                Ok(output)
            }
            FileSyncAction::Sync => {
                let urls = if self.urls.is_empty() {
                    let futures = FileSyncCache::get_cache_list(&pool)
                        .await?
                        .into_iter()
                        .map(|v| async move { v.delete_cache_entry(&pool).await });
                    let result: Result<Vec<_>, Error> = try_join_all(futures).await;
                    result?;

                    FileSyncConfig::get_url_list(&pool).await?
                } else {
                    self.urls.to_vec()
                };
                let futures = urls.into_iter().map(|url| {
                    let blacklist = blacklist.clone();
                    let pool = pool.clone();
                    async move {
                        let mut flist = FileList::from_url(&url, &config, &pool).await?;
                        let list = flist.fill_file_list().await?;
                        let list: Vec<_> = if blacklist.could_be_in_blacklist(&url) {
                            list.into_par_iter()
                                .filter(|entry| {
                                    if let Some(url) = entry.urlname.as_ref() {
                                        !blacklist.is_in_blacklist(url)
                                    } else {
                                        true
                                    }
                                })
                                .collect()
                        } else {
                            list
                        };
                        flist.with_list(list);
                        flist.cache_file_list()?;
                        Ok(flist)
                    }
                });
                let flists: Result<Vec<_>, Error> = try_join_all(futures).await;
                let flists = flists?;

                let futures = flists.chunks(2).map(|f| async move {
                    if f.len() == 2 {
                        FileSync::compare_lists(&(*f[0]), &(*f[1]), &pool).await?;
                    }
                    Ok(())
                });
                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                results?;

                let results: Vec<_> = FileSyncCache::get_cache_list(&pool)
                    .await?
                    .into_iter()
                    .map(|entry| format!("{} {}", entry.src_url, entry.dst_url))
                    .collect();
                output.extend_from_slice(&results);
                Ok(output)
            }
            FileSyncAction::Copy => {
                if self.urls.len() < 2 {
                    Err(format_err!("Need 2 Urls"))
                } else {
                    let finfo0 = FileInfo::from_url(&self.urls[0])?;
                    let finfo1 = FileInfo::from_url(&self.urls[1])?;

                    if finfo1.servicetype == FileService::Local {
                        let flist = FileList::from_url(&self.urls[0], &config, &pool).await?;
                        FileSync::copy_object(&(*flist), &finfo0, &finfo1).await?;
                        Ok(output)
                    } else {
                        let flist = FileList::from_url(&self.urls[1], &config, &pool).await?;
                        FileSync::copy_object(&(*flist), &finfo0, &finfo1).await?;
                        Ok(output)
                    }
                }
            }
            FileSyncAction::List => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    for urls in group_urls(&self.urls).values() {
                        let mut flist = FileList::from_url(&urls[0], &config, &pool).await?;
                        for url in urls {
                            flist.set_baseurl(url.clone());
                            flist.print_list().await?;
                        }
                    }
                    Ok(output)
                }
            }
            FileSyncAction::Process => {
                let fsync = FileSync::new(config.clone());
                fsync.process_sync_cache(&pool).await?;
                Ok(output)
            }
            FileSyncAction::Delete => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let fsync = FileSync::new(config.clone());
                    fsync.delete_files(&self.urls, &pool).await?;
                    Ok(output)
                }
            }
            FileSyncAction::Move => {
                if self.urls.len() == 2 {
                    let finfo0 = FileInfo::from_url(&self.urls[0])?;
                    let finfo1 = FileInfo::from_url(&self.urls[1])?;

                    if finfo0.servicetype == finfo1.servicetype {
                        let flist = FileList::from_url(&self.urls[0], &config, &pool).await?;
                        flist.move_file(&finfo0, &finfo1).await?;
                        Ok(output)
                    } else {
                        Err(format_err!("Can only move within servicetype"))
                    }
                } else {
                    Err(format_err!("Need 2 Urls"))
                }
            }
            FileSyncAction::Serialize => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let futures = self.urls.iter().map(|url| async move {
                        let pool = pool.clone();
                        let mut flist = FileList::from_url(&url, &config, &pool).await?;
                        flist.with_list(flist.fill_file_list().await?);
                        let results: Result<Vec<_>, Error> = flist
                            .get_filemap()
                            .values()
                            .map(|finfo| serde_json::to_string(finfo.inner()).map_err(Into::into))
                            .collect();
                        results
                    });
                    let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                    let results: Vec<_> = results?.into_iter().flatten().collect();
                    output.extend_from_slice(&results);
                    Ok(output)
                }
            }
            FileSyncAction::AddConfig => {
                if self.urls.len() == 2 {
                    InsertFileSyncConfig::insert_config(
                        &pool,
                        self.urls[0].as_str(),
                        self.urls[1].as_str(),
                    )
                    .await
                    .map(|_| ())?;
                    Ok(output)
                } else {
                    Err(format_err!("Need exactly 2 Urls"))
                }
            }
            FileSyncAction::ShowCache => {
                let clist: Vec<_> = FileSyncCache::get_cache_list(&pool)
                    .await?
                    .into_iter()
                    .map(|v| format!("{} {}", v.src_url, v.dst_url))
                    .collect();
                output.push(clist.join("\n"));
                Ok(output)
            }
            FileSyncAction::SyncGarmin => {
                let sync = GarminSync::new(config.clone());
                output.extend_from_slice(&sync.run_sync().await?);
                Ok(output)
            }
            FileSyncAction::SyncMovie => {
                let sync = MovieSync::new(config.clone());
                output.extend_from_slice(&sync.run_sync().await?);
                Ok(output)
            }
            FileSyncAction::SyncCalendar => {
                let sync = CalendarSync::new(config.clone());
                output.extend_from_slice(&sync.run_sync().await?);
                Ok(output)
            }
        }
    }
}
