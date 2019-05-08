use failure::{err_msg, Error};
use structopt::StructOpt;
use url::Url;

use crate::config::Config;
use crate::file_info::FileInfo;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
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

        match opts.action {
            FileSyncAction::Sync => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let config = Config::new();
                    let pool = PgPool::new(&config.database_url);

                    let fsync = FileSync::new(opts.mode);
                    let conf0 = FileListConf::from_url(opts.urls[0].clone())?;
                    let conf1 = FileListConf::from_url(opts.urls[1].clone())?;
                    let flist0 = FileList::from_conf(conf0);
                    let flist1 = FileList::from_conf(conf1);
                    let flist0 = flist0.with_list(&flist0.fill_file_list(Some(&pool))?);
                    let flist1 = flist1.with_list(&flist1.fill_file_list(Some(&pool))?);
                    flist0.cache_file_list(&pool)?;
                    flist1.cache_file_list(&pool)?;
                    fsync.compare_lists(&flist0, &flist1)
                }
            }
            FileSyncAction::Copy => {
                if opts.urls.len() < 2 {
                    Err(err_msg("Need 2 Urls"))
                } else {
                    let fsync = FileSync::new(opts.mode);
                    let conf0 = FileListConf::from_url(opts.urls[0].clone())?;
                    let conf1 = FileListConf::from_url(opts.urls[1].clone())?;
                    let flist0 = FileList::from_conf(conf0);
                    let flist1 = FileList::from_conf(conf1);
                    let finfo0 = FileInfo::from_url(opts.urls[0].clone())?;
                    let finfo1 = FileInfo::from_url(opts.urls[1].clone())?;
                    fsync.copy_object(&flist0, &flist1, &finfo0, &finfo1)
                }
            }
            FileSyncAction::List => {
                if opts.urls.len() < 1 {
                    Err(err_msg("Need at least 1 Url"))
                } else {
                    let config = Config::new();
                    let pool = PgPool::new(&config.database_url);

                    for url in opts.urls {
                        let conf = FileListConf::from_url(url.clone())?;
                        let flist = FileList::from_conf(conf);
                        let flist = flist.with_list(&flist.fill_file_list(Some(&pool))?);
                        flist.cache_file_list(&pool)?;
                        for f in flist.get_filemap().values() {
                            println!("{:?}", f);
                        }
                    }
                    Ok(())
                }
            }
            FileSyncAction::Process => {
                let fsync = FileSync::new(opts.mode);
                fsync.process_file()
            }
            _ => Ok(()),
        }
    }
}
