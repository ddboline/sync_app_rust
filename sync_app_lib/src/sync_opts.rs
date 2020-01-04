use failure::{err_msg, Error};
use log::debug;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use std::io::{stdout, Write};
use structopt::StructOpt;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoSerialize, FileInfoTrait};
use crate::file_list::{group_urls, FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::file_sync::{FileSync, FileSyncAction};
use crate::models::{BlackList, FileSyncCache, FileSyncConfig, InsertFileSyncConfig};
use crate::pgpool::PgPool;

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(parse(try_from_str))]
    /// Available commands are: "index", "sync", "proc(ess)", "copy" or "cp", "list" or "ls",
    /// "delete" or "rm", "move" or "mv", "ser" or "serialize", "add" or "add_config",
    /// "show" or "show_cache"
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

    pub fn process_args() -> Result<(), Error> {
        let opts = SyncOpts::from_args();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        opts.process_sync_opts(&config, &pool)
    }

    pub fn process_sync_opts(&self, config: &Config, pool: &PgPool) -> Result<(), Error> {
        let blacklist = BlackList::new(pool)?;

        match self.action {
            FileSyncAction::Index => {
                let urls = if self.urls.is_empty() {
                    FileSyncConfig::get_url_list(&pool)?
                } else {
                    self.urls.to_vec()
                };
                debug!("urls: {:?}", urls);
                let results: Result<Vec<_>, Error> = urls
                    .par_iter()
                    .map(|url| {
                        let pool = pool.clone();
                        let conf = FileListConf::from_url(&url, &config)?;
                        let flist = FileList::from_conf(conf);
                        let list = flist.fill_file_list(Some(&pool))?;
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
                        let flist = flist.with_list(list);
                        flist.cache_file_list(&pool)?;
                        Ok(flist)
                    })
                    .collect();

                results.map(|_| ())
            }
            FileSyncAction::Sync => {
                let urls = if self.urls.is_empty() {
                    let result: Result<(), Error> = FileSyncCache::get_cache_list(&pool)?
                        .into_par_iter()
                        .map(|v| v.delete_cache_entry(&pool))
                        .collect();
                    result?;

                    FileSyncConfig::get_url_list(&pool)?
                } else {
                    self.urls.to_vec()
                };

                let results: Result<Vec<_>, Error> = urls
                    .par_iter()
                    .map(|url| {
                        let pool = pool.clone();
                        let conf = FileListConf::from_url(&url, &config)?;
                        let flist = FileList::from_conf(conf);
                        let list = flist.fill_file_list(Some(&pool))?;
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
                        let flist = flist.with_list(list);
                        flist.cache_file_list(&pool)?;
                        Ok(flist)
                    })
                    .collect();

                let flists = results?;

                let results: Result<Vec<_>, Error> = flists
                    .into_par_iter()
                    .chunks(2)
                    .map(|f| {
                        if f.len() == 2 {
                            FileSync::compare_lists(&f[0], &f[1], &pool)?;
                        }
                        Ok(())
                    })
                    .collect();
                results?;

                for entry in FileSyncCache::get_cache_list(&pool)? {
                    writeln!(stdout().lock(), "{} {}", entry.src_url, entry.dst_url)?;
                }
                Ok(())
            }
            FileSyncAction::Copy => {
                if self.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let finfo0 = FileInfo::from_url(&self.urls[0])?;
                    let finfo1 = FileInfo::from_url(&self.urls[1])?;

                    if finfo1.servicetype == FileService::Local {
                        let conf = FileListConf::from_url(&self.urls[0], &config)?;
                        let flist = FileList::from_conf(conf);
                        FileSync::copy_object(&flist, &finfo0, &finfo1)
                    } else {
                        let conf = FileListConf::from_url(&self.urls[1], &config)?;
                        let flist = FileList::from_conf(conf);
                        FileSync::copy_object(&flist, &finfo0, &finfo1)
                    }
                }
            }
            FileSyncAction::List => {
                if self.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    for urls in group_urls(&self.urls).values() {
                        let conf = FileListConf::from_url(&urls[0], &config)?;
                        let mut flist = FileList::from_conf(conf);
                        for url in urls {
                            flist.conf.baseurl = url.clone();
                            flist.print_list()?;
                        }
                    }
                    Ok(())
                }
            }
            FileSyncAction::Process => {
                let fsync = FileSync::new(config.clone());
                fsync.process_sync_cache(&pool)
            }
            FileSyncAction::Delete => {
                if self.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let fsync = FileSync::new(config.clone());
                    fsync.delete_files(&self.urls, &pool)
                }
            }
            FileSyncAction::Move => {
                if self.urls.len() != 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let conf0 = FileListConf::from_url(&self.urls[0], &config)?;
                    let conf1 = FileListConf::from_url(&self.urls[1], &config)?;
                    if conf0.servicetype != conf1.servicetype {
                        Err(err_msg("Can only move within servicetype"))
                    } else {
                        Ok(())
                    }
                }
            }
            FileSyncAction::Serialize => {
                if self.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let results: Result<Vec<_>, Error> = self
                        .urls
                        .par_iter()
                        .map(|url| {
                            let pool = pool.clone();
                            let conf = FileListConf::from_url(&url, &config)?;
                            let flist = FileList::from_conf(conf);
                            let flist = flist.with_list(flist.fill_file_list(Some(&pool))?);
                            let results: Result<Vec<_>, Error> = flist
                                .filemap
                                .values()
                                .map(|finfo| {
                                    let tmp: FileInfoSerialize = finfo.clone().into();
                                    let js = serde_json::to_string(&tmp)?;
                                    writeln!(stdout().lock(), "{}", js)?;
                                    Ok(())
                                })
                                .collect();
                            results.map(|_| ())
                        })
                        .collect();
                    results.map(|_| ())
                }
            }
            FileSyncAction::AddConfig => {
                if self.urls.len() != 2 {
                    Err(err_msg("Need exactly 2 Urls"))
                } else {
                    InsertFileSyncConfig::insert_config(
                        &pool,
                        self.urls[0].as_str(),
                        self.urls[1].as_str(),
                    )
                    .map(|_| ())
                }
            }
            FileSyncAction::ShowCache => {
                let clist: Vec<_> = FileSyncCache::get_cache_list(&pool)?
                    .into_iter()
                    .map(|v| format!("{} {}", v.src_url, v.dst_url))
                    .collect();
                let body = clist.join("\n");
                writeln!(stdout().lock(), "{}", body)?;
                Ok(())
            }
        }
    }
}
