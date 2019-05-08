use diesel::prelude::*;
use failure::{err_msg, Error};
use std::convert::Into;
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ToString;
use url::Url;

use crate::file_info_local::FileInfoLocal;
use crate::file_info_s3::FileInfoS3;
use crate::file_service::FileService;
use crate::map_parse;
use crate::models::{FileInfoCache, InsertFileInfoCache};
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct FileStat {
    pub st_mtime: u32,
    pub st_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Md5Sum(pub String);

impl FromStr for Md5Sum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 32 {
            Err(err_msg(format!("Invalid md5sum {}", s)))
        } else {
            Ok(Self(s.to_string()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sha1Sum(pub String);

impl FromStr for Sha1Sum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 40 {
            Err(err_msg(format!("Invalid sha1sum {}", s)))
        } else {
            Ok(Self(s.to_string()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceId(pub String);

impl From<String> for ServiceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSession(pub String);

impl FromStr for ServiceSession {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(err_msg("Session name must not be empty"))
        } else {
            Ok(Self(s.to_string()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
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
    fn get_finfo(&self) -> &FileInfo;
    fn get_md5(&self) -> Option<Md5Sum>;
    fn get_sha1(&self) -> Option<Sha1Sum>;
    fn get_stat(&self) -> Option<FileStat>;
}

impl FileInfoTrait for FileInfo {
    fn get_finfo(&self) -> &FileInfo {
        &self
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        self.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.filestat
    }
}

impl FileInfo {
    pub fn from_url(url: Url) -> Result<FileInfo, Error> {
        match url.scheme() {
            "file" => FileInfoLocal::from_url(url).map(|x| x.0),
            "s3" => FileInfoS3::from_url(url).map(|x| x.0),
            _ => Err(err_msg("Bad scheme")),
        }
    }

    pub fn from_cache_info(item: FileInfoCache) -> Result<FileInfo, Error> {
        Ok(FileInfo {
            filename: item.filename,
            filepath: item.filepath.map(Into::into),
            urlname: match item.urlname {
                Some(urlname) => match urlname.parse() {
                    Ok(urlname) => Some(urlname),
                    _ => None,
                },
                None => None,
            },
            md5sum: map_parse(item.md5sum)?,
            sha1sum: map_parse(item.sha1sum)?,
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
            serviceid: item.serviceid.map(Into::into),
            servicetype: item.servicetype.parse()?,
            servicesession: map_parse(item.servicesession)?,
        })
    }

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

#[cfg(test)]
mod tests {
    use crate::file_info::{map_parse, ServiceSession};

    #[test]
    fn test_map_parse() {
        let test_sessionstr: Option<_> = Some("test_sessionname".to_string());
        let test_sessionname: Option<ServiceSession> = map_parse(test_sessionstr).unwrap();

        assert_eq!(
            test_sessionname,
            Some(ServiceSession("test_sessionname".to_string()))
        );
    }
}
