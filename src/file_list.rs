use diesel::prelude::*;
use failure::{err_msg, Error};
use rayon::prelude::*;
use std::collections::HashMap;
use url::Url;

use crate::file_info::{FileInfo, ServiceId, ServiceSession};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::models::FileInfoCache;
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

#[derive(Debug, Clone)]
pub struct FileListConf {
    pub baseurl: Url,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
    pub serviceid: ServiceId,
}

impl FileListConf {
    pub fn from_url(url: Url) -> FileListConf {
        
    }
}

#[derive(Debug, Clone)]
pub struct FileList {
    pub conf: FileListConf,
    pub filemap: HashMap<String, FileInfo>,
}

impl FileList {
    pub fn from_conf(conf: FileListConf) -> FileList {
        FileList {
            conf,
            filemap: HashMap::new(),
        }
    }

    pub fn with_list(&self, filelist: &[FileInfo]) -> FileList {
        FileList {
            conf: self.conf.clone(),
            filemap: filelist
                .iter()
                .map(|f| (f.filename.clone(), f.clone()))
                .collect(),
        }
    }
}

pub trait FileListTrait {
    fn get_conf(&self) -> &FileListConf;

    fn get_filemap(&self) -> &HashMap<String, FileInfo>;

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error>;

    fn upload_file(&self, finfo_local: &FileInfo, finfo_remote: &FileInfo) -> Result<(), Error>;

    fn download_file(&self, finfo_remote: &FileInfo, finfo_local: &FileInfo) -> Result<(), Error>;

    fn cache_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
        let conn = pool.get()?;

        let flist_cache: Vec<Result<_, Error>> = self
            .get_filemap()
            .par_iter()
            .map(|(_, f)| {
                let finfo_cache = f.get_cache_info()?;
                Ok(finfo_cache)
            })
            .collect();

        let flist_cache = map_result_vec(flist_cache)?;

        diesel::insert_into(file_info_cache::table)
            .values(&flist_cache)
            .execute(&conn)
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

    fn get_file_list_dict(&self, pool: &PgPool) -> Result<HashMap<String, FileInfo>, Error> {
        let flist_dict: HashMap<_, _> = self
            .load_file_list(&pool)?
            .into_par_iter()
            .filter_map(|entry| {
                if entry.filepath.is_none() {
                    None
                } else {
                    let key = entry.filepath.as_ref().unwrap().to_string();
                    let val = FileInfo::from_cache_info(entry);
                    if val.is_err() {
                        None
                    } else {
                        Some((key, val.unwrap()))
                    }
                }
            })
            .collect();
        Ok(flist_dict)
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

impl FileListTrait for FileList {
    fn get_conf(&self) -> &FileListConf {
        &self.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.filemap
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        match pool {
            Some(pool) => match self.load_file_list(&pool) {
                Ok(v) => {
                    let result: Vec<Result<_, Error>> =
                        v.into_iter().map(FileInfo::from_cache_info).collect();
                    map_result_vec(result)
                }
                Err(e) => Err(e),
            },
            None => Ok(Vec::new()),
        }
    }

    fn upload_file(&self, _: &FileInfo, _: &FileInfo) -> Result<(), Error> {
        Err(err_msg("Not implemented for base FileInfo"))
    }

    fn download_file(&self, _: &FileInfo, _: &FileInfo) -> Result<(), Error> {
        Err(err_msg("Not implemented for base FileInfo"))
    }
}

pub fn replace_baseurl(urlname: &Url, baseurl0: &Url, baseurl1: &Url) -> Result<Url, Error> {
    let baseurl0 = format!("{}/", baseurl0.as_str().trim_end_matches('/'));
    let baseurl1 = baseurl1.as_str().trim_end_matches('/');

    let urlstr = format!("{}/{}", baseurl1, urlname.as_str().replace(&baseurl0, ""));
    Url::parse(&urlstr).map_err(err_msg)
}
