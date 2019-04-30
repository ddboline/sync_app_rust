use failure::{err_msg, Error};
use reqwest::Url;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::time::SystemTime;
use subprocess::Exec;
use walkdir::DirEntry;

use crate::file_info::{
    FileInfo, FileInfoTrait, FileStat, Md5Sum, ServiceId, ServiceSession, Sha1Sum,
};
use crate::file_service::FileService;

pub struct FileInfoLocal(pub FileInfo);

impl FileInfoTrait for FileInfoLocal {
    fn get_md5(&self) -> Option<Md5Sum> {
        match self.0.filepath.as_ref() {
            Some(p) => match p.to_str() {
                Some(f) => _get_md5sum(f).ok().map(Md5Sum),
                None => None,
            },
            None => None,
        }
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        match self.0.filepath.as_ref() {
            Some(p) => match p.to_str() {
                Some(f) => _get_sha1sum(f).ok().map(Sha1Sum),
                None => None,
            },
            None => None,
        }
    }

    fn get_stat(&self) -> Option<FileStat> {
        match self.0.filepath.as_ref() {
            Some(p) => _get_stat(&p).ok(),
            None => None,
        }
    }

    fn get_service_id(&self) -> Option<ServiceId> {
        self.0.get_service_id()
    }
}

fn _get_md5sum(filename: &str) -> Result<String, Error> {
    let command = format!("md5sum {}", filename);

    let stream = Exec::shell(command).stream_stdout()?;

    let reader = BufReader::new(stream);

    if let Some(line) = reader.lines().next() {
        if let Some(entry) = line?.split_whitespace().next() {
            Ok(entry.to_string())
        } else {
            Ok("".to_string())
        }
    } else {
        Ok("".to_string())
    }
}

fn _get_sha1sum(filename: &str) -> Result<String, Error> {
    let command = format!("sha1sum {}", filename);

    let stream = Exec::shell(command).stream_stdout()?;

    let reader = BufReader::new(stream);

    if let Some(line) = reader.lines().next() {
        if let Some(entry) = line?.split_whitespace().next() {
            Ok(entry.to_string())
        } else {
            Ok("".to_string())
        }
    } else {
        Ok("".to_string())
    }
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
    pub fn from_direntry(
        item: DirEntry,
        serviceid: Option<ServiceId>,
        servicesession: Option<ServiceSession>,
    ) -> Result<FileInfoLocal, Error> {
        if item.file_type().is_dir() {
            return Err(err_msg("Is a directory, skipping"));
        }
        let filename = item
            .path()
            .file_name()
            .ok_or_else(|| err_msg("Parse failure"))?
            .to_os_string()
            .into_string()
            .map_err(|_| err_msg("Parse failure"))?;
        let filestat = match item.metadata() {
            Ok(metadata) => {
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
            _ => None,
        };
        let filepath = item.path().canonicalize()?;
        let filestr = filepath
            .to_str()
            .ok_or_else(|| err_msg("Failed to parse path"))?;
        let fileurl: Url = format!("file://{}", filestr).parse().unwrap();
        let md5sum = _get_md5sum(filestr).ok().map(Md5Sum);
        let sha1sum = _get_sha1sum(filestr).ok().map(Sha1Sum);

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
}
