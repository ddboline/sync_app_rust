use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use failure::{err_msg, Error};
use std::convert::Into;
use std::path::PathBuf;
use std::str::FromStr;
use std::string::ToString;
use url::Url;

use crate::file_info_gdrive::FileInfoGDrive;
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

impl From<ServiceSession> for ServiceId {
    fn from(s: ServiceSession) -> Self {
        Self(s.0)
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

pub enum FileInfoKeyType {
    FileName,
    FilePath,
    UrlName,
    Md5Sum,
    Sha1Sum,
    ServiceId,
}

pub trait FileInfoTrait
where
    Self: Sized + Send + Sync,
{
    fn from_url(url: &Url) -> Result<Self, Error>;
    fn get_finfo(&self) -> &FileInfo;
    fn into_finfo(self) -> FileInfo;
    fn get_md5(&self) -> Option<Md5Sum>;
    fn get_sha1(&self) -> Option<Sha1Sum>;
    fn get_stat(&self) -> Option<FileStat>;
}

impl FileInfoTrait for FileInfo {
    fn from_url(url: &Url) -> Result<FileInfo, Error> {
        match url.scheme() {
            "file" => FileInfoLocal::from_url(url).map(FileInfoTrait::into_finfo),
            "s3" => FileInfoS3::from_url(url).map(FileInfoTrait::into_finfo),
            "gdrive" => FileInfoGDrive::from_url(url).map(FileInfoTrait::into_finfo),
            _ => Err(err_msg("Bad scheme")),
        }
    }

    fn get_finfo(&self) -> &FileInfo {
        &self
    }

    fn into_finfo(self) -> FileInfo {
        self
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
    pub fn from_cache_info(item: &FileInfoCache) -> Result<FileInfo, Error> {
        Ok(FileInfo {
            filename: item.filename.clone(),
            filepath: item.filepath.clone().map(Into::into),
            urlname: match item.urlname.as_ref() {
                Some(urlname) => match urlname.parse() {
                    Ok(urlname) => Some(urlname),
                    _ => None,
                },
                None => None,
            },
            md5sum: map_parse(&item.md5sum)?,
            sha1sum: map_parse(&item.sha1sum)?,
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
            serviceid: item.serviceid.clone().map(Into::into),
            servicetype: item.servicetype.parse()?,
            servicesession: map_parse(&item.servicesession)?,
        })
    }

    pub fn from_database(pool: &PgPool, url: &Url) -> Result<Option<FileInfo>, Error> {
        use crate::schema::file_info_cache::dsl::*;

        let conn = pool.get()?;

        let result = match file_info_cache
            .filter(urlname.eq(url.as_str().to_string()))
            .load::<FileInfoCache>(&conn)
            .map_err(err_msg)?
            .get(0)
        {
            Some(f) => Some(FileInfo::from_cache_info(&f)?),
            None => None,
        };

        Ok(result)
    }
}

impl From<&FileInfo> for InsertFileInfoCache {
    fn from(item: &FileInfo) -> Self {
        Self {
            filename: item.filename.clone(),
            filepath: match item.filepath.as_ref() {
                Some(f) => f.to_str().map(ToString::to_string),
                None => None,
            },
            urlname: item.urlname.as_ref().map(Url::to_string),
            md5sum: item.md5sum.as_ref().map(|m| m.0.clone()),
            sha1sum: item.sha1sum.as_ref().map(|s| s.0.clone()),
            filestat_st_mtime: item.filestat.map(|f| f.st_mtime as i32),
            filestat_st_size: item.filestat.map(|f| f.st_size as i32),
            serviceid: item.serviceid.as_ref().map(|s| s.0.clone()),
            servicetype: item.servicetype.to_string(),
            servicesession: item.servicesession.as_ref().map(|s| s.0.clone()),
        }
    }
}

impl From<FileInfo> for InsertFileInfoCache {
    fn from(item: FileInfo) -> Self {
        Self::from(&item)
    }
}

pub fn cache_file_info(pool: &PgPool, finfo: FileInfo) -> Result<FileInfoCache, Error> {
    let conn = pool.get()?;

    let finfo_cache: InsertFileInfoCache = finfo.into();

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
        let test_sessionname: Option<ServiceSession> = map_parse(&test_sessionstr).unwrap();

        assert_eq!(
            test_sessionname,
            Some(ServiceSession("test_sessionname".to_string()))
        );
    }
}
