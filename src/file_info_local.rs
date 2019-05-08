use checksums::{hash_file, Algorithm};
use failure::{err_msg, Error};
use std::fs;
use std::fs::File;
use std::fs::Metadata;
use std::path::Path;
use std::time::SystemTime;
use url::Url;
use walkdir::DirEntry;

use crate::file_info::{
    FileInfo, FileInfoTrait, FileStat, Md5Sum, ServiceId, ServiceSession, Sha1Sum,
};
use crate::file_service::FileService;

#[derive(Debug, Clone)]
pub struct FileInfoLocal(pub FileInfo);

impl FileInfoTrait for FileInfoLocal {
    fn get_finfo(&self) -> &FileInfo {
        &self.0
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        match self.0.filepath.as_ref() {
            Some(p) => _get_md5sum(&p).ok().map(Md5Sum),
            None => None,
        }
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        match self.0.filepath.as_ref() {
            Some(p) => _get_sha1sum(&p).ok().map(Sha1Sum),
            None => None,
        }
    }

    fn get_stat(&self) -> Option<FileStat> {
        match self.0.filepath.as_ref() {
            Some(p) => _get_stat(&p).ok(),
            None => None,
        }
    }
}

fn _get_md5sum(path: &Path) -> Result<String, Error> {
    {
        File::open(path)?;
    }
    Ok(hash_file(path, Algorithm::MD5).to_lowercase())
}

fn _get_sha1sum(path: &Path) -> Result<String, Error> {
    {
        File::open(path)?;
    }
    Ok(hash_file(path, Algorithm::SHA1).to_lowercase())
}

fn _get_stat(p: &Path) -> Result<FileStat, Error> {
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
    pub fn from_url(url: Url) -> Result<FileInfoLocal, Error> {
        if url.scheme() != "file" {
            Err(err_msg("Wrong scheme"))
        } else {
            let path = url.to_file_path().map_err(|_| err_msg("Parse failure"))?;
            let filename = path
                .file_name()
                .ok_or_else(|| err_msg("Parse failure"))?
                .to_os_string()
                .into_string()
                .map_err(|_| err_msg("Parse failure"))?;
            let finfo = FileInfo {
                filename,
                filepath: Some(path),
                urlname: Some(url),
                md5sum: None,
                sha1sum: None,
                filestat: None,
                serviceid: None,
                servicetype: FileService::Local,
                servicesession: None,
            };
            Ok(FileInfoLocal(finfo))
        }
    }

    pub fn from_path_and_metadata(
        path: &Path,
        metadata: Option<Metadata>,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<FileInfoLocal, Error> {
        if path.is_dir() {
            return Err(err_msg("Is a directory, skipping"));
        }
        let filename = path
            .file_name()
            .ok_or_else(|| err_msg("Parse failure"))?
            .to_os_string()
            .into_string()
            .map_err(|_| err_msg("Parse failure"))?;
        let filestat = match metadata {
            Some(metadata) => {
                let modified = metadata
                    .modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs() as i64;
                let size = metadata.len();
                Some(FileStat {
                    st_mtime: modified as u32,
                    st_size: size as u32,
                })
            }
            None => None,
        };

        let filepath = path.canonicalize()?;
        let fileurl =
            Url::from_file_path(filepath.clone()).map_err(|_| err_msg("Failed to parse url"))?;
        let md5sum = _get_md5sum(&filepath).ok().map(Md5Sum);
        let sha1sum = _get_sha1sum(&filepath).ok().map(Sha1Sum);

        let finfo = FileInfo {
            filename,
            filepath: Some(filepath),
            urlname: Some(fileurl),
            md5sum,
            sha1sum,
            filestat,
            serviceid,
            servicetype: FileService::Local,
            servicesession,
        };
        Ok(FileInfoLocal(finfo))
    }

    pub fn from_path(
        path: &Path,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<FileInfoLocal, Error> {
        if path.is_dir() {
            return Err(err_msg("Is a directory, skipping"));
        }
        let metadata = path.metadata().ok();
        FileInfoLocal::from_path_and_metadata(&path, metadata, serviceid, servicesession)
    }

    pub fn from_direntry(
        item: DirEntry,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<FileInfoLocal, Error> {
        if item.file_type().is_dir() {
            return Err(err_msg("Is a directory, skipping"));
        }
        let path = item.path();
        let metadata = item.metadata().ok();
        FileInfoLocal::from_path_and_metadata(&path, metadata, serviceid, servicesession)
    }
}
