use failure::{err_msg, Error};
use std::fs::File;
use std::io::{BufRead, BufReader};
use structopt::StructOpt;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait};
use crate::file_list::{group_urls, FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_sync::{FileSync, FileSyncAction, FileSyncMode};
use crate::map_result_vec;
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
                    let mut all_urls = opts.urls.clone();
                    if let FileSyncMode::OutputFile(fname) = &opts.mode {
                        let f = File::open(fname)?;
                        let proc_list: Vec<Result<_, Error>> = BufReader::new(f)
                            .lines()
                            .map(|line| {
                                let url: Url = match line?
                                    .split_whitespace()
                                    .map(ToString::to_string)
                                    .nth(0)
                                {
                                    Some(v) => v.parse()?,
                                    None => return Err(err_msg("No url")),
                                };
                                Ok(url)
                            })
                            .collect();
                        let proc_list = map_result_vec(proc_list)?;
                        all_urls.extend_from_slice(&proc_list);
                    }
                    for urls in group_urls(&all_urls).values() {
                        let conf = FileListConf::from_url(&urls[0], &config)?;
                        let flist = FileList::from_conf(conf);
                        let fdict = flist.get_file_list_dict(
                            flist.load_file_list(&pool)?,
                            FileInfoKeyType::UrlName,
                        );
                        for url in urls {
                            let finfo = if let Some(f) = fdict.get(&url.as_str().to_string()) {
                                f.clone()
                            } else {
                                FileInfo::from_url(&url)?
                            };

                            println!("delete {:?}", finfo);
                            flist.delete(&finfo)?;
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
