use anyhow::{format_err, Error};
use clap::Parser;
use futures::{future::try_join_all, TryStreamExt};
use itertools::Itertools;
use log::{debug, info};
use refinery::embed_migrations;
use stack_string::{format_sstr, StackString};
use std::{convert::TryInto, path::PathBuf};
use stdout_channel::StdoutChannel;
use tokio::{
    fs::File,
    io::{stdout as tokio_stdout, AsyncWrite, AsyncWriteExt},
};
use url::Url;
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use crate::{
    calendar_sync::CalendarSync,
    config::Config,
    file_info::FileInfo,
    file_list::{group_urls, FileList},
    file_service::FileService,
    file_sync::{FileSync, FileSyncAction},
    garmin_sync::GarminSync,
    models::{FileInfoCache, FileSyncCache, FileSyncConfig},
    movie_sync::MovieSync,
    pgpool::PgPool,
    security_sync::SecuritySync,
    weather_sync::WeatherSync,
};

embed_migrations!("../migrations");

fn action_from_str(s: &str) -> Result<FileSyncAction, String> {
    s.parse().map_err(|e| format!("{e}"))
}

fn url_from_str(s: &str) -> Result<Url, String> {
    s.parse().map_err(|e| format!("{e}"))
}

#[derive(Parser, Debug)]
pub struct SyncOpts {
    #[clap(value_parser = action_from_str)]
    /// Available commands are: `index`, `sync`, `proc(ess)`, `copy` or `cp`,
    /// `list` or `ls`, `delete` or `rm`, `move` or `mv`, `ser` or
    /// `serialize`, `add` or `add_config`, `show`, `show_cache`
    /// `sync_garmin`, `sync_movie`, `sync_calendar`, `show_config`,
    /// `sync_all`, `run-migrations`, `sync_weather`
    pub action: FileSyncAction,
    #[clap(short = 'u', long = "urls", value_parser = url_from_str)]
    pub urls: Vec<Url>,
    #[clap(short = 'o', long = "offset")]
    pub offset: Option<usize>,
    #[clap(short = 'l', long = "limit")]
    pub limit: Option<usize>,
    #[clap(short = 'n', long = "name")]
    pub name: Option<StackString>,
    #[clap(short = 'd', long)]
    pub show_deleted: bool,
    #[clap(short = 'f', long)]
    pub filename: Option<PathBuf>,
}

impl Default for SyncOpts {
    fn default() -> Self {
        Self {
            action: FileSyncAction::ShowCache,
            urls: Vec::new(),
            offset: None,
            limit: None,
            name: None,
            show_deleted: false,
            filename: None,
        }
    }
}

impl SyncOpts {
    #[must_use]
    pub fn new(action: FileSyncAction, urls: &[Url]) -> Self {
        Self {
            action,
            urls: urls.into(),
            ..Self::default()
        }
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn process_args() -> Result<StdoutChannel<StackString>, Error> {
        let stdout = StdoutChannel::new();
        let opts = Self::parse();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;

        if opts.action == FileSyncAction::SyncAll {
            for action in &[
                FileSyncAction::Sync,
                FileSyncAction::SyncGarmin,
                FileSyncAction::SyncMovie,
                FileSyncAction::SyncCalendar,
                FileSyncAction::SyncSecurity,
                FileSyncAction::SyncWeather,
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

    /// # Errors
    /// Return error if db query fails
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::similar_names)]
    pub async fn process_sync_opts(
        &self,
        config: &Config,
        pool: &PgPool,
        stdout: &StdoutChannel<StackString>,
    ) -> Result<(), Error> {
        match self.action {
            FileSyncAction::Index => {
                let url_list: Vec<_>;
                let urls = if self.urls.is_empty() {
                    url_list = FileSyncConfig::get_url_list(pool).await?;
                    &url_list
                } else {
                    &self.urls
                };
                info!("urls: {urls:?}",);
                let futures = urls.iter().map(|url| {
                    let pool = pool.clone();
                    async move {
                        let flist = FileList::from_url(url, config, &pool).await?;
                        let number_updated = flist.update_file_cache().await?;
                        info!("indexed {url} updated {number_updated}");
                        Ok(())
                    }
                });
                let result: Result<Vec<()>, Error> = try_join_all(futures).await;
                result?;
                Ok(())
            }
            FileSyncAction::Sync => {
                let urls = if self.urls.is_empty() || self.name.is_some() {
                    let result: Result<(), Error> = FileSyncCache::get_cache_list(pool)
                        .await?
                        .map_err(Into::into)
                        .try_for_each(|v| async move {
                            v.delete_cache_entry(pool).await?;
                            Ok(())
                        })
                        .await;
                    result?;
                    if let Some(name) = self.name.as_ref() {
                        let v = FileSyncConfig::get_by_name(pool, name)
                            .await?
                            .ok_or_else(|| format_err!("Name does not exist"))?;
                        let u0: Url = v.src_url.parse()?;
                        let u1: Url = v.dst_url.parse()?;
                        vec![u0, u1]
                    } else {
                        FileSyncConfig::get_url_list(pool).await?
                    }
                } else {
                    self.urls.clone()
                };
                debug!("Check 0");

                let futures = urls.into_iter().map(|url| {
                    let pool = pool.clone();
                    async move {
                        let flist = FileList::from_url(&url, config, &pool).await?;
                        debug!("start {url}");
                        let number_updated = flist.update_file_cache().await?;
                        debug!("cached {url} updated {number_updated}");
                        Ok(flist)
                    }
                });
                let results: Result<Vec<_>, Error> = try_join_all(futures).await;
                let flists = results?;
                debug!("Check 1");
                let futures = flists.chunks(2).map(|f| async move {
                    if f.len() == 2 {
                        FileSync::compare_lists(&(*f[0]), &(*f[1]), pool).await?;
                    }
                    Ok(())
                });
                let results: Result<Vec<()>, Error> = try_join_all(futures).await;
                results?;
                debug!("Check 2");
                let mut stream = Box::pin(FileSyncCache::get_cache_list(pool).await?);
                while let Some(entry) = stream.try_next().await? {
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
                        let flist = FileList::from_url(url, config, &pool).await?;
                        let count = FileInfoCache::count_cached(
                            flist.get_servicesession().as_str(),
                            flist.get_servicetype().to_str(),
                            &pool,
                            self.show_deleted,
                        )
                        .await?;
                        let buf = format_sstr!("{url}\t{count}");
                        stdout.send(buf);
                        Ok(())
                    });
                    let results: Result<Vec<()>, Error> = try_join_all(futures).await;
                    results?;
                    Ok(())
                }
            }
            FileSyncAction::Serialize => {
                if self.urls.is_empty() {
                    Err(format_err!("Need at least 1 Url"))
                } else {
                    let mut file: Box<dyn AsyncWrite + Unpin + Send> =
                        if let Some(filename) = &self.filename {
                            Box::new(File::create(&filename).await?)
                        } else {
                            Box::new(tokio_stdout())
                        };
                    for url in &self.urls {
                        let flist = FileList::from_url(url, config, pool).await?;
                        let list: Result<Vec<FileInfo>, Error> = flist
                            .load_file_list(self.show_deleted)
                            .await?
                            .into_iter()
                            .map(TryInto::try_into)
                            .collect();
                        let list = list?;

                        let offset = self.offset.unwrap_or(0);
                        let limit = self.limit.unwrap_or(list.len());

                        for finfo in list
                            .iter()
                            .sorted_by_key(|finfo| finfo.filepath.as_path())
                            .skip(offset)
                            .take(limit)
                        {
                            file.write_all(&serde_json::to_vec(finfo.inner())?).await?;
                            file.write_all(b"\n").await?;
                        }
                    }
                    Ok(())
                }
            }
            FileSyncAction::AddConfig => {
                if self.urls.len() == 2 {
                    let conf = FileSyncConfig {
                        id: Uuid::new_v4(),
                        src_url: self.urls[0].as_str().into(),
                        dst_url: self.urls[1].as_str().into(),
                        last_run: DateTimeWrapper::now(),
                        name: self.name.clone(),
                    };
                    conf.insert_config(pool).await?;
                    Ok(())
                } else {
                    Err(format_err!("Need exactly 2 Urls"))
                }
            }
            FileSyncAction::ShowConfig => {
                let entries: Vec<_> = FileSyncConfig::get_config_list(pool)
                    .await?
                    .map_ok(|v| {
                        format_sstr!("{} {} {}", v.src_url, v.dst_url, v.name.unwrap_or_default())
                    })
                    .try_collect()
                    .await?;
                let clist = entries.join("\n");
                stdout.send(clist);
                Ok(())
            }
            FileSyncAction::ShowCache => {
                let clist: Vec<_> = FileSyncCache::get_cache_list(pool)
                    .await?
                    .map_ok(|v| {
                        let buf = format_sstr!("{} {}", v.src_url, v.dst_url);
                        buf
                    })
                    .try_collect()
                    .await?;
                let clist = clist.join("\n");
                stdout.send(clist);
                Ok(())
            }
            FileSyncAction::SyncGarmin => {
                let sync = GarminSync::new(config.clone())?;
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncMovie => {
                let sync = MovieSync::new(config.clone())?;
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncCalendar => {
                let sync = CalendarSync::new(config.clone())?;
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncSecurity => {
                let sync = SecuritySync::new(config.clone())?;
                for line in sync.run_sync().await? {
                    stdout.send(line);
                }
                Ok(())
            }
            FileSyncAction::SyncWeather => {
                let sync = WeatherSync::new(config.clone())?;
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
