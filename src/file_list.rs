use diesel::prelude::*;
use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::path::PathBuf;

use crate::file_info::{FileInfo, ServiceSession};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::models::FileInfoCache;
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

pub struct FileListConf {
    pub basedir: PathBuf,
    pub baseurl: Url,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
}

pub struct FileList {
    pub conf: FileListConf,
    pub filelist: Vec<FileInfo>,
}

pub trait FileListTrait {
    fn fill_file_list(conf: FileListConf) -> Result<FileList, Error>;
}

pub fn cache_file_list(pool: &PgPool, flist: &FileList) -> Result<Vec<FileInfoCache>, Error> {
    let conn = pool.get()?;

    let flist_cache: Vec<Result<_, Error>> = flist
        .filelist
        .par_iter()
        .map(|f| {
            let finfo_cache = f.get_cache_info()?;
            Ok(finfo_cache)
        })
        .collect();

    let flist_cache = map_result_vec(flist_cache)?;

    diesel::insert_into(file_info_cache::table)
        .values(&flist_cache)
        .get_results(&conn)
        .map_err(err_msg)
}
