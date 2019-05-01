use diesel::prelude::*;
use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::path::PathBuf;

use crate::file_info::{FileInfo, ServiceId, ServiceSession};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::models::FileInfoCache;
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

#[derive(Debug, Clone)]
pub struct FileListConf {
    pub basedir: PathBuf,
    pub baseurl: Url,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
    pub serviceid: ServiceId,
}

#[derive(Debug)]
pub struct FileList {
    pub conf: FileListConf,
    pub filelist: Vec<FileInfo>,
}

pub trait FileListTrait {
    fn get_conf(&self) -> &FileListConf;

    fn get_filelist(&self) -> &[FileInfo];

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error>;

    fn cache_file_list(&self, pool: &PgPool) -> Result<Vec<FileInfoCache>, Error> {
        let conn = pool.get()?;

        let flist_cache: Vec<Result<_, Error>> = self
            .get_filelist()
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

    fn load_file_list(&self, pool: &PgPool) -> Result<Vec<FileInfoCache>, Error> {
        use crate::schema::file_info_cache::dsl::*;

        let conn = pool.get()?;
        let conf = &self.get_conf();

        file_info_cache
            .filter(serviceid.eq(conf.serviceid.0.clone()))
            .filter(servicesession.eq(conf.servicesession.0.clone()))
            .filter(servicetype.eq(conf.servicetype.to_string()))
            .load::<FileInfoCache>(&conn)
            .map_err(err_msg)
    }

    fn clear_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
        use crate::schema::file_info_cache::dsl::*;

        let conn = pool.get()?;
        let conf = &self.get_conf();

        diesel::delete(
            file_info_cache
                .filter(serviceid.eq(conf.serviceid.0.clone()))
                .filter(servicesession.eq(conf.servicesession.0.clone()))
                .filter(servicetype.eq(conf.servicetype.to_string())),
        )
        .execute(&conn)
        .map_err(err_msg)
    }
}
