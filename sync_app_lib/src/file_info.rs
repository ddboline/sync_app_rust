use anyhow::{format_err, Error};
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::{
    convert::{Into, TryFrom, TryInto},
    fmt::Debug,
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    string::ToString,
    sync::Arc,
};
use url::Url;

use crate::{
    file_info_gdrive::FileInfoGDrive,
    file_info_local::FileInfoLocal,
    file_info_s3::FileInfoS3,
    file_info_ssh::FileInfoSSH,
    file_service::FileService,
    map_parse,
    models::{FileInfoCache, InsertFileInfoCache},
    path_buf_wrapper::PathBufWrapper,
    pgpool::PgPool,
    schema::file_info_cache,
    stack_string::StackString,
    url_wrapper::UrlWrapper,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStat {
    pub st_mtime: u32,
    pub st_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Md5Sum(pub StackString);

impl From<StackString> for Md5Sum {
    fn from(s: StackString) -> Self {
        Self(s)
    }
}

impl FromStr for Md5Sum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 32 {
            Ok(Self(s.into()))
        } else {
            Err(format_err!("Invalid md5sum {}", s))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sha1Sum(pub StackString);

impl From<StackString> for Sha1Sum {
    fn from(s: StackString) -> Self {
        Self(s)
    }
}

impl FromStr for Sha1Sum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 40 {
            Ok(Self(s.into()))
        } else {
            Err(format_err!("Invalid sha1sum {}", s))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceId(pub StackString);

impl From<StackString> for ServiceId {
    fn from(s: StackString) -> Self {
        Self(s)
    }
}

impl From<String> for ServiceId {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

impl From<&str> for ServiceId {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

impl From<ServiceSession> for ServiceId {
    fn from(s: ServiceSession) -> Self {
        Self(s.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceSession(pub StackString);

impl FromStr for ServiceSession {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(format_err!("Session name must not be empty"))
        } else {
            Ok(Self(s.into()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FileInfoInner {
    pub filename: StackString,
    pub filepath: Option<PathBufWrapper>,
    pub urlname: Option<UrlWrapper>,
    pub md5sum: Option<Md5Sum>,
    pub sha1sum: Option<Sha1Sum>,
    pub filestat: Option<FileStat>,
    pub serviceid: Option<ServiceId>,
    pub servicetype: FileService,
    pub servicesession: Option<ServiceSession>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FileInfo(Arc<FileInfoInner>);

impl Deref for FileInfo {
    type Target = FileInfoInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub enum FileInfoKeyType {
    FileName,
    FilePath,
    UrlName,
    Md5Sum,
    Sha1Sum,
    ServiceId,
}

pub trait FileInfoTrait: Send + Sync + Debug {
    fn get_finfo(&self) -> &FileInfo;
    fn into_finfo(self) -> FileInfo;
    fn get_md5(&self) -> Option<Md5Sum>;
    fn get_sha1(&self) -> Option<Sha1Sum>;
    fn get_stat(&self) -> Option<FileStat>;
}

impl FileInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        filename: StackString,
        filepath: Option<PathBufWrapper>,
        urlname: Option<UrlWrapper>,
        md5sum: Option<Md5Sum>,
        sha1sum: Option<Sha1Sum>,
        filestat: Option<FileStat>,
        serviceid: Option<ServiceId>,
        servicetype: FileService,
        servicesession: Option<ServiceSession>,
    ) -> Self {
        let inner = FileInfoInner {
            filename,
            filepath,
            urlname,
            md5sum,
            sha1sum,
            filestat,
            serviceid,
            servicetype,
            servicesession,
        };
        Self(Arc::new(inner))
    }

    pub fn from_inner(inner: FileInfoInner) -> Self {
        Self(Arc::new(inner))
    }

    pub fn inner(&self) -> &FileInfoInner {
        &self.0
    }

    pub fn from_url(url: &Url) -> Result<Self, Error> {
        match url.scheme() {
            "file" => FileInfoLocal::from_url(url).map(FileInfoTrait::into_finfo),
            "s3" => FileInfoS3::from_url(url).map(FileInfoTrait::into_finfo),
            "gdrive" => FileInfoGDrive::from_url(url).map(FileInfoTrait::into_finfo),
            "ssh" => FileInfoSSH::from_url(url).map(FileInfoTrait::into_finfo),
            _ => Err(format_err!("Bad scheme")),
        }
    }
}

impl FileInfoTrait for FileInfo {
    fn get_finfo(&self) -> &Self {
        &self
    }

    fn into_finfo(self) -> Self {
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

impl TryFrom<&FileInfoCache> for FileInfo {
    type Error = Error;
    fn try_from(item: &FileInfoCache) -> Result<Self, Self::Error> {
        let inner = FileInfoInner {
            filename: item.filename.clone(),
            filepath: item.filepath.as_ref().map(|f| f.as_str().into()),
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
            serviceid: item.serviceid.as_ref().map(|s| s.as_str().into()),
            servicetype: item.servicetype.parse()?,
            servicesession: map_parse(&item.servicesession)?,
        };
        Ok(Self(Arc::new(inner)))
    }
}

impl FileInfo {
    pub fn from_database(pool: &PgPool, url: &Url) -> Result<Option<Self>, Error> {
        use crate::schema::file_info_cache::dsl::{file_info_cache, urlname};

        let conn = pool.get()?;

        let result = match file_info_cache
            .filter(urlname.eq(url.as_str().to_string()))
            .load::<FileInfoCache>(&conn)?
            .get(0)
        {
            Some(f) => Some(f.try_into()?),
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
                Some(f) => Some(f.to_string_lossy().as_ref().into()),
                None => None,
            },
            urlname: item.urlname.as_ref().map(|s| s.as_str().into()),
            md5sum: item.md5sum.as_ref().map(|m| m.0.clone()),
            sha1sum: item.sha1sum.as_ref().map(|s| s.0.clone()),
            filestat_st_mtime: item.filestat.map(|f| f.st_mtime as i32),
            filestat_st_size: item.filestat.map(|f| f.st_size as i32),
            serviceid: item.serviceid.as_ref().map(|s| s.0.clone()),
            servicetype: item.servicetype.to_string().into(),
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
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use crate::{
        file_info::{map_parse, ServiceSession},
        stack_string::StackString,
    };

    #[test]
    fn test_map_parse() {
        let test_sessionstr: Option<StackString> = Some("test_sessionname".into());
        let test_sessionname: Option<ServiceSession> = map_parse(&test_sessionstr).unwrap();

        assert_eq!(
            test_sessionname,
            Some(ServiceSession("test_sessionname".into()))
        );
    }
}
