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
        let fileurl = format!("gdrive://{}/{}", bucket, path).parse()?;
        let serviceid = Some(bucket.to_string().into());
        let servicesession = Some(bucket.parse()?);

        let finfo = FileInfo {
            filename,
            filepath: Some(filepath.to_path_buf()),
            urlname: Some(fileurl),
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
    pub fn from_object(bucket: &str, item: Object) -> Result<FileInfoGDrive, Error> {
        let key = item.key.as_ref().ok_or_else(|| err_msg("No key"))?.clone();
        let filepath = Path::new(&key);
        let filename = filepath
            .file_name()
            .ok_or_else(|| err_msg("Parse failure"))?
            .to_os_string()
            .into_string()
            .map_err(|_| err_msg("Parse failure"))?;
        let md5sum = item
            .e_tag
            .clone()
            .and_then(|m| m.trim_matches('"').parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.last_modified
                .as_ref()
                .ok_or_else(|| err_msg("No last modified"))?,
        )?
        .timestamp();
        let size = item.size.ok_or_else(|| err_msg("No file size"))?;
        let fileurl = format!("gdrive://{}/{}", bucket, key).parse()?;
        let serviceid = Some(bucket.to_string().into());
        let servicesession = Some(bucket.parse()?);

        let finfo = FileInfo {
            filename,
            filepath: Some(filepath.to_path_buf()),
            urlname: Some(fileurl),
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
    use crate::file_info_s3::FileInfoGDrive;

    #[test]
    fn test_file_info_gdrive() {
        assert!(false);
    }
}
