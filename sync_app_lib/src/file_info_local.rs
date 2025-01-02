use anyhow::{format_err, Error};
use checksums::{hash_file, Algorithm};
use std::{
    fs,
    fs::{File, Metadata},
    path::Path,
    time::SystemTime,
};
use url::Url;
use walkdir::DirEntry;

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, ServiceId, ServiceSession, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Clone)]
pub struct FileInfoLocal(pub FileInfo);

impl FileInfoLocal {
    /// # Errors
    /// Return error if init fails
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|e| format_err!("Parse failure {e:?}"))?;

            let filename = path
                .file_name()
                .ok_or_else(|| format_err!("Parse failure"))?
                .to_string_lossy()
                .into_owned()
                .into();
            let finfo = FileInfo::new(
                filename,
                path.into(),
                url.clone().into(),
                None,
                None,
                FileStat::default(),
                ServiceId::default(),
                FileService::Local,
                ServiceSession::default(),
            );
            Ok(Self(finfo))
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }
}

impl FileInfoTrait for FileInfoLocal {
    fn get_finfo(&self) -> &FileInfo {
        &self.0
    }

    fn into_finfo(self) -> FileInfo {
        self.0
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        get_md5sum_impl(&self.0.filepath)
            .ok()
            .and_then(|s| s.parse().ok())
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        get_sha1sum_impl(&self.0.filepath)
            .ok()
            .and_then(|s| s.parse().ok())
    }

    fn get_stat(&self) -> FileStat {
        get_stat_impl(&self.0.filepath).unwrap_or_default()
    }
}

fn get_md5sum_impl(path: &Path) -> Result<String, Error> {
    {
        File::open(path)?;
    }
    Ok(hash_file(path, Algorithm::MD5).to_lowercase())
}

fn get_sha1sum_impl(path: &Path) -> Result<String, Error> {
    {
        File::open(path)?;
    }
    Ok(hash_file(path, Algorithm::SHA1).to_lowercase())
}

fn get_stat_impl(p: &Path) -> Result<FileStat, Error> {
    let metadata = fs::metadata(p)?;

    let modified = metadata
        .modified()?
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs() as i64;
    let size = metadata.len();

    Ok(FileStat {
        st_mtime: modified as u32,
        st_size: size as u32,
    })
}

impl FileInfoLocal {
    /// # Errors
    /// Return error if init fails
    pub fn from_path_and_metadata(
        path: &Path,
        metadata: Option<Metadata>,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<Self, Error> {
        if path.is_dir() {
            return Err(format_err!("Is a directory, skipping"));
        }
        let filename = path
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_string_lossy()
            .into_owned()
            .into();
        let filestat = {
            let metadata = metadata.ok_or_else(|| format_err!("No metadata"))?;
            let modified = metadata
                .modified()?
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64;
            let size = metadata.len();
            FileStat {
                st_mtime: modified as u32,
                st_size: size as u32,
            }
        };
        let serviceid = serviceid.ok_or_else(|| format_err!("No service id"))?;
        let servicesession = servicesession.ok_or_else(|| format_err!("No servicesession"))?;

        let filepath = path.canonicalize()?;
        let fileurl = Url::from_file_path(filepath.clone())
            .map_err(|e| format_err!("Failed to parse url {e:?}"))?;
        let md5sum = get_md5sum_impl(&filepath).ok().and_then(|s| s.parse().ok());
        let sha1sum = get_sha1sum_impl(&filepath)
            .ok()
            .and_then(|s| s.parse().ok());

        let finfo = FileInfo::new(
            filename,
            filepath.into(),
            fileurl.into(),
            md5sum,
            sha1sum,
            filestat,
            serviceid,
            FileService::Local,
            servicesession,
        );
        Ok(Self(finfo))
    }

    /// # Errors
    /// Return error if init fails
    pub fn from_path(
        path: &Path,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<Self, Error> {
        if path.is_dir() {
            return Err(format_err!("Is a directory, skipping"));
        }
        let metadata = path.metadata().ok();
        Self::from_path_and_metadata(path, metadata, serviceid, servicesession)
    }

    /// # Errors
    /// Return error if init fails
    pub fn from_direntry(
        item: &DirEntry,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<Self, Error> {
        if item.file_type().is_dir() {
            return Err(format_err!("Is a directory, skipping"));
        }
        let path = item.path();
        let metadata = item.metadata().ok();
        Self::from_path_and_metadata(path, metadata, serviceid, servicesession)
    }
}
