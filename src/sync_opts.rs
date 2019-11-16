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
use crate::models::{FileSyncCache, FileSyncConfig, InsertFileSyncConfig};
use crate::pgpool::PgPool;

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(parse(try_from_str))]
    pub action: FileSyncAction,
    #[structopt(short = "u", long = "urls", parse(try_from_str))]
    pub urls: Vec<Url>,
}

impl SyncOpts {
    pub fn process_args() -> Result<(), Error> {
        let opts = SyncOpts::from_args();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        match opts.action {
            FileSyncAction::Index => {
                let urls = if opts.urls.is_empty() {
                    FileSyncConfig::get_url_list(&pool)?
                } else {
                    opts.urls
                };
                debug!("urls: {:?}", urls);
                let results: Result<Vec<_>, Error> = urls
                    .par_iter()
                    .map(|url| {
                        let pool = pool.clone();
                        let conf = FileListConf::from_url(&url, &config)?;
                        let flist = FileList::from_conf(conf);
                        let flist = flist.with_list(flist.fill_file_list(Some(&pool))?);
                        flist.cache_file_list(&pool)?;
                        Ok(flist)
                    })
                    .collect();

                results.map(|_| ())
            }
            FileSyncAction::Sync => {
                let urls = if opts.urls.is_empty() {
                    let result: Result<(), Error> = FileSyncCache::get_cache_list(&pool)?
                        .into_iter()
                        .map(|v| v.delete_cache_entry(&pool))
                        .collect();
                    result?;

                    FileSyncConfig::get_url_list(&pool)?
                } else {
                    opts.urls
                };

                let fsync = FileSync::new(config.clone());

                let results: Result<Vec<_>, Error> = urls
                    .par_iter()
                    .map(|url| {
                        let pool = pool.clone();
                        let conf = FileListConf::from_url(&url, &config)?;
                        let flist = FileList::from_conf(conf);
                        let flist = flist.with_list(flist.fill_file_list(Some(&pool))?);
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
                            fsync.compare_lists(&f[0], &f[1], &pool)?;
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
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let fsync = FileSync::new(config.clone());

                    let finfo0 = FileInfo::from_url(&opts.urls[0])?;
                    let finfo1 = FileInfo::from_url(&opts.urls[1])?;

                    if finfo1.servicetype == FileService::Local {
                        let conf = FileListConf::from_url(&opts.urls[0], &config)?;
                        let flist = FileList::from_conf(conf);
                        fsync.copy_object(&flist, &finfo0, &finfo1)
                    } else {
                        let conf = FileListConf::from_url(&opts.urls[1], &config)?;
                        let flist = FileList::from_conf(conf);
                        fsync.copy_object(&flist, &finfo0, &finfo1)
                    }
                }
            }
            FileSyncAction::List => {
                if opts.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    for urls in group_urls(&opts.urls).values() {
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
                let pool = PgPool::new(&config.database_url);
                let fsync = FileSync::new(config);
                fsync.process_sync_cache(&pool)
            }
            FileSyncAction::Delete => {
                if opts.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let pool = PgPool::new(&config.database_url);
                    let fsync = FileSync::new(config);
                    fsync.delete_files(&opts.urls, &pool)
                }
            }
            FileSyncAction::Move => {
                if opts.urls.len() != 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let conf0 = FileListConf::from_url(&opts.urls[0], &config)?;
                    let conf1 = FileListConf::from_url(&opts.urls[1], &config)?;
                    if conf0.servicetype != conf1.servicetype {
                        Err(err_msg("Can only move within servicetype"))
                    } else {
                        Ok(())
                    }
                }
            }
            FileSyncAction::Serialize => {
                if opts.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let pool = PgPool::new(&config.database_url);

                    let results: Result<Vec<_>, Error> = opts
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
                if opts.urls.len() != 2 {
                    Err(err_msg("Need exactly 2 Urls"))
                } else {
                    let pool = PgPool::new(&config.database_url);

                    InsertFileSyncConfig::insert_config(
                        &pool,
                        opts.urls[0].as_str(),
                        opts.urls[1].as_str(),
                    )
                    .map(|_| ())
                }
            }
        }
    }
}
