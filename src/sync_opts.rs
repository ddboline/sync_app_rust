use failure::{err_msg, Error};
use structopt::StructOpt;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_list::{group_urls, FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_sync::{FileSync, FileSyncAction, FileSyncMode};
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
        let config = Config::new();

        match opts.action {
            FileSyncAction::Sync => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let pool = PgPool::new(&config.database_url);

                    let fsync = FileSync::new(opts.mode, &config);
                    let conf0 = FileListConf::from_url(&opts.urls[0], &config)?;
                    let conf1 = FileListConf::from_url(&opts.urls[1], &config)?;
                    let flist0 = FileList::from_conf(conf0);
                    let flist1 = FileList::from_conf(conf1);
                    let flist0 = flist0.with_list(&flist0.fill_file_list(Some(&pool))?);
                    flist0.cache_file_list(&pool)?;
                    let flist1 = flist1.with_list(&flist1.fill_file_list(Some(&pool))?);
                    flist1.cache_file_list(&pool)?;
                    fsync.compare_lists(&flist0, &flist1)
                }
            }
            FileSyncAction::Copy => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let fsync = FileSync::new(opts.mode, &config);
                    let conf0 = FileListConf::from_url(&opts.urls[0], &config)?;
                    let conf1 = FileListConf::from_url(&opts.urls[1], &config)?;
                    let flist0 = FileList::from_conf(conf0);
                    let flist1 = FileList::from_conf(conf1);
                    let finfo0 = FileInfo::from_url(&opts.urls[0])?;
                    let finfo1 = FileInfo::from_url(&opts.urls[1])?;
                    fsync.copy_object(&flist0, &flist1, &finfo0, &finfo1)
                }
            }
            FileSyncAction::List => {
                if opts.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    for (_, urls) in &group_urls(&opts.urls) {
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
                let fsync = FileSync::new(opts.mode, &config);
                fsync.process_file()
            }
            FileSyncAction::Delete => {
                if opts.urls.is_empty() {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    for (_, urls) in &group_urls(&opts.urls) {
                        for url in urls {
                            let finfo = FileInfo::from_url(&url)?;
                            finfo.delete()?;
                        }
                    }
                    println!("{:?}", group_urls(&opts.urls));
                    Ok(())
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
