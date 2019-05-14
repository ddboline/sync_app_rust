use google_drive3_fork as drive3;

use chrono::DateTime;
use failure::{err_msg, Error};
use std::path::Path;
use url::Url;

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum};
use crate::file_service::FileService;

#[derive(Debug, Default)]
pub struct FileInfoGDrive(pub FileInfo);

impl FileInfoTrait for FileInfoGDrive {
    fn from_url(url: &Url) -> Result<FileInfoGDrive, Error> {
        if url.scheme() != "gdrive" {
            return Err(err_msg("Invalid URL"));
        }
        let path = url.path();
        let filepath = Path::new(&path);
        let filename = filepath
            .file_name()
            .ok_or_else(|| err_msg("Parse failure"))?
            .to_os_string()
            .into_string()
            .map_err(|_| err_msg("Parse failure"))?;
        let serviceid = Some(filename.clone().into());
        let servicesession = Some(filename.parse()?);

        let finfo = FileInfo {
            filename,
            filepath: Some(filepath.to_path_buf()),
            urlname: Some(url.clone()),
            md5sum: None,
            sha1sum: None,
            filestat: None,
            serviceid,
            servicetype: FileService::GDrive,
            servicesession,
        };
        Ok(FileInfoGDrive(finfo))
    }

    fn get_finfo(&self) -> &FileInfo {
        &self.0
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        self.0.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.0.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.0.filestat
    }
}

impl FileInfoGDrive {
    pub fn from_object(item: drive3::File) -> Result<FileInfoGDrive, Error> {
        let filename = item.name.ok_or_else(|| err_msg("No filename"))?;
        let md5sum = item.md5_checksum.and_then(|x| x.parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.modified_time
                .as_ref()
                .ok_or_else(|| err_msg("No last modified"))?,
        )?
        .timestamp();
        let size: u32 = item
            .size
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| err_msg("Failed to parse"))?;
        let serviceid = item.id.map(|x| x.into());
        let servicesession = Some("gdrive".parse()?);

        let finfo = FileInfo {
            filename,
            filepath: None,
            urlname: None,
            md5sum,
            sha1sum: None,
            filestat: Some(FileStat {
                st_mtime: st_mtime as u32,
                st_size: size as u32,
            }),
            serviceid,
            servicetype: FileService::GDrive,
            servicesession,
        };

        Ok(FileInfoGDrive(finfo))
    }
}

#[cfg(test)]
mod tests {
    use crate::file_info_gdrive::FileInfoGDrive;

    #[test]
    fn test_file_info_gdrive() {
        assert!(false);
    }
}
