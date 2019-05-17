use google_drive3_fork as drive3;

use chrono::DateTime;
use failure::{err_msg, Error};
use std::collections::HashMap;
use std::path::Path;
use url::Url;

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum};
use crate::file_service::FileService;
use crate::gdrive_instance::{DirectoryInfo, GDriveInstance};

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
    pub fn from_object(
        item: drive3::File,
        gdrive: &GDriveInstance,
        directory_map: &HashMap<String, DirectoryInfo>,
    ) -> Result<FileInfoGDrive, Error> {
        let filename = item.name.as_ref().ok_or_else(|| err_msg("No filename"))?;
        let md5sum = item.md5_checksum.as_ref().and_then(|x| x.parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.modified_time
                .as_ref()
                .ok_or_else(|| err_msg("No last modified"))?,
        )?
        .timestamp();
        let size: u32 = item.size.as_ref().and_then(|x| x.parse().ok()).unwrap_or(0);
        let serviceid = item.id.as_ref().map(|x| x.clone().into());
        let servicesession = Some("gdrive".parse()?);

        let export_path = gdrive.get_export_path(&item, &directory_map)?;
        let filepath = Path::new(&export_path).to_path_buf();
        let urlname: Url = format!("gdrive:///{}", export_path).parse()?;

        let finfo = FileInfo {
            filename: filename.clone(),
            filepath: Some(filepath),
            urlname: Some(urlname),
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
    use url::Url;

    use crate::file_info::FileInfoTrait;
    use crate::file_info_gdrive::FileInfoGDrive;
    use crate::file_service::FileService;

    #[test]
    fn test_file_info_gdrive() {
        let url: Url = "gdrive:///My Drive/test.txt".parse().unwrap();
        let finfo = FileInfoGDrive::from_url(&url).unwrap();
        println!("{:?}", finfo);
        assert_eq!(finfo.0.filename, "test.txt");
        assert_eq!(finfo.0.servicetype, FileService::GDrive);
    }
}
