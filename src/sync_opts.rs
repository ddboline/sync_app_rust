use failure::{err_msg, Error};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use structopt::StructOpt;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_list::{group_urls, FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_sync::{FileSync, FileSyncAction, FileSyncMode};
use crate::map_result;
use crate::pgpool::PgPool;

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(short = "m", long = "mode", parse(from_str), default_value = "full")]
    mode: FileSyncMode,
    #[structopt(parse(try_from_str))]
    action: FileSyncAction,
    #[structopt(short = "u", long = "urls", parse(try_from_str))]
    urls: Vec<Url>,
}

impl SyncOpts {
    pub fn process_args() -> Result<(), Error> {
        let opts = SyncOpts::from_args();
        let config = Config::init_config()?;

        match opts.action {
            FileSyncAction::Sync => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let pool = PgPool::new(&config.database_url);

                    let fsync = FileSync::new(opts.mode, &config);

                    let results: Vec<Result<_, Error>> = opts.urls[0..2]
                        .par_iter()
                        .map(|url| {
                            let pool = pool.clone();
                            let conf = FileListConf::from_url(&url, &config)?;
                            let flist = FileList::from_conf(conf);
                            let flist = flist.with_list(&flist.fill_file_list(Some(&pool))?);
                            flist.cache_file_list(&pool)?;
                            Ok(flist)
                        })
                        .collect();

                    let flists: Vec<_> = map_result(results)?;

                    fsync.compare_lists(&flists[0], &flists[1])
                }
            }
            FileSyncAction::Copy => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let fsync = FileSync::new(opts.mode, &config);
                    let conf = FileListConf::from_url(&opts.urls[0], &config)?;
                    let flist = FileList::from_conf(conf);
                    let finfo0 = FileInfo::from_url(&opts.urls[0])?;
                    let finfo1 = FileInfo::from_url(&opts.urls[1])?;
                    fsync.copy_object(&flist, &finfo0, &finfo1)
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
                            println!("url {:?} {}", url, flist.get_conf().servicetype);
                            flist.conf.baseurl = url.clone();
                            flist.print_list()?;
                        }
                    }
                    Ok(())
                }
            }
            FileSyncAction::Process => {
                let pool = PgPool::new(&config.database_url);
                let fsync = FileSync::new(opts.mode, &config);
                fsync.process_file(&pool)
            }
            FileSyncAction::Delete => {
                if opts.urls.is_empty() && opts.mode == FileSyncMode::Full {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let pool = PgPool::new(&config.database_url);
                    let fsync = FileSync::new(opts.mode, &config);
                    fsync.delete_files(&opts.urls, &pool)
                }
            }
            FileSyncAction::Move => {
                if opts.urls.len() < 2 {
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
        }
    }
}
