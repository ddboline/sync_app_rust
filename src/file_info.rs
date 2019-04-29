use diesel::prelude::*;
use failure::{err_msg, Error};
use reqwest::Url;
use std::convert::Into;
use std::path::PathBuf;
use std::string::ToString;

use crate::file_service::FileService;
use crate::models::{FileInfoCache, InsertFileInfoCache};
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

#[derive(Copy, Clone, Debug)]
pub struct FileStat {
    pub st_mtime: u32,
    pub st_size: u32,
}

#[derive(Clone, Debug)]
pub struct Md5Sum(pub String);

#[derive(Clone, Debug)]
pub struct Sha1Sum(pub String);

#[derive(Clone, Debug)]
pub struct ServiceId(pub String);

#[derive(Clone, Debug)]
pub struct ServiceSession(pub String);

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub filename: String,
    pub filepath: Option<PathBuf>,
    pub urlname: Option<Url>,
    pub md5sum: Option<Md5Sum>,
    pub sha1sum: Option<Sha1Sum>,
    pub filestat: Option<FileStat>,
    pub serviceid: Option<ServiceId>,
    pub servicetype: FileService,
    pub servicesession: Option<ServiceSession>,
}

pub trait FileInfoTrait {
    fn get_md5(&self) -> Option<Md5Sum>;
    fn get_sha1(&self) -> Option<Sha1Sum>;
    fn get_stat(&self) -> Option<FileStat>;
    fn get_service_id(&self) -> Option<ServiceId>;
}

impl FileInfoTrait for FileInfo {
    fn get_md5(&self) -> Option<Md5Sum> {
        self.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.filestat
    }

    fn get_service_id(&self) -> Option<ServiceId> {
        None
    }
}

impl From<FileInfoCache> for FileInfo {
    fn from(item: FileInfoCache) -> Self {
        Self {
            filename: item.filename,
            filepath: item.filepath.map(Into::into),
            urlname: match item.urlname {
                Some(urlname) => match urlname.parse() {
                    Ok(urlname) => Some(urlname),
                    _ => None,
                },
                None => None,
            },
            md5sum: item.md5sum.map(Md5Sum),
            sha1sum: item.sha1sum.map(Sha1Sum),
            filestat: match item.filestat_st_mtime {
                Some(st_mtime) => match item.filestat_st_size {
                    Some(st_size) => Some(FileStat {
                        st_mtime: st_mtime as u32,
                        st_size: st_size as u32,
                    }),
                    None => None,
                },
                None => None,
            },
            serviceid: item.serviceid.map(ServiceId),
            servicetype: match item.servicetype.parse() {
                Ok(s) => s,
                _ => FileService::Local,
            },
            servicesession: item.servicesession.map(ServiceSession),
        }
    }
}

impl FileInfo {
    pub fn get_cache_info(&self) -> Result<InsertFileInfoCache, Error> {
        let finfo_cache = InsertFileInfoCache {
            filename: self.filename.clone(),
            filepath: match self.filepath.as_ref() {
                Some(f) => f.to_str().map(ToString::to_string),
                None => None,
            },
            urlname: self.urlname.as_ref().map(Url::to_string),
            md5sum: self.md5sum.as_ref().map(|m| m.0.clone()),
            sha1sum: self.sha1sum.as_ref().map(|s| s.0.clone()),
            filestat_st_mtime: self.filestat.map(|f| f.st_mtime as i32),
            filestat_st_size: self.filestat.map(|f| f.st_size as i32),
            serviceid: self.serviceid.as_ref().map(|s| s.0.clone()),
            servicetype: self.servicetype.to_string(),
            servicesession: self.servicesession.as_ref().map(|s| s.0.clone()),
        };
        Ok(finfo_cache)
    }
}

pub fn cache_file_info(pool: &PgPool, finfo: &FileInfo) -> Result<FileInfoCache, Error> {
    let conn = pool.get()?;

    let finfo_cache = finfo.get_cache_info()?;

    diesel::insert_into(file_info_cache::table)
        .values(&finfo_cache)
        .get_result(&conn)
        .map_err(err_msg)
}
