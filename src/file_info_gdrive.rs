use google_drive3_fork as drive3;

use chrono::DateTime;
use failure::{err_msg, Error};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use url::Url;

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum};
use crate::file_service::FileService;
use crate::gdrive_instance::{DirectoryInfo, GDriveInstance};

#[derive(Debug, Default)]
pub struct FileInfoGDrive {
    pub finfo: FileInfo,
}

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
        Ok(FileInfoGDrive { finfo })
    }

    fn get_finfo(&self) -> &FileInfo {
        &self.finfo
    }

    fn into_finfo(self) -> FileInfo {
        self.finfo
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        self.finfo.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.finfo.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.finfo.filestat
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
        let servicesession = Some(gdrive.session_name.parse()?);

        let export_path = gdrive.get_export_path(&item, &directory_map)?;
        let filepath = export_path.iter().fold(PathBuf::new(), |mut p, e| {
            p.push(e);
            p
        });
        let urlname = format!("gdrive://{}/", gdrive.session_name);
        let urlname = Url::parse(&urlname)?;
        let urlname = export_path.iter().fold(urlname, |u, e| {
            if e.contains('#') {
                u.join(&e.replace("#", "%35")).unwrap()
            } else {
                u.join(e).unwrap()
            }
        });

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

        Ok(FileInfoGDrive { finfo })
    }
}

#[cfg(test)]
mod tests {
    use google_drive3_fork as drive3;
    use url::Url;

    use crate::config::Config;
    use crate::file_info::FileInfoTrait;
    use crate::file_info_gdrive::FileInfoGDrive;
    use crate::file_service::FileService;
    use crate::gdrive_instance::GDriveInstance;

    #[test]
    fn test_file_info_gdrive() {
        let url: Url = "gdrive:///My Drive/test.txt".parse().unwrap();
        let finfo = FileInfoGDrive::from_url(&url).unwrap();
        println!("{:?}", finfo);
        assert_eq!(finfo.get_finfo().filename, "test.txt");
        assert_eq!(finfo.get_finfo().servicetype, FileService::GDrive);
    }

    #[test]
    fn test_file_info_from_object() {
        let config = Config::new();
        let gdrive = GDriveInstance::new(&config, "ddboline@gmail.com");
        let (dmap, _) = gdrive.get_directory_map().unwrap();
        let f = drive3::File {
            mime_type: Some("application/pdf".to_string()),
            viewed_by_me_time: Some("2019-04-20T21:18:40.865Z".to_string()),
            id: Some("1M6EzRPGaJBaZgN_2bUQPcgKY2o7JXJvb".to_string()),
            size: Some("859249".to_string()),
            parents: Some(vec!["0ABGM0lfCdptnUk9PVA".to_string()]),
            md5_checksum: Some("2196a214fd7eccc6292adb96602f5827".to_string()),
            modified_time: Some("2019-04-20T21:18:40.865Z".to_string()),
            created_time: Some("2019-04-20T21:18:40.865Z".to_string()),
            owners: Some(vec![drive3::User { me: Some(true),
            kind: Some("drive#user".to_string()),
            display_name: Some("Daniel Boline".to_string()),
            photo_link: Some("https://lh5.googleusercontent.com/-dHefkGDbPx4/AAAAAAAAAAI/AAAAAAAAUik/4rvsDcSqY0U/s64/photo.jpg".to_string()),
            email_address: Some("ddboline@gmail.com".to_string()),
            permission_id: Some("15472502093706922513".to_string()) }]),
            name: Some("armstrong_thesis_2003.pdf".to_string()),
            web_content_link: Some("https://drive.google.com/uc?id=1M6EzRPGaJBaZgN_2bUQPcgKY2o7JXJvb&export=download".to_string()),
            trashed: Some(false),
            file_extension: Some("pdf".to_string()),
            ..Default::default()
        };

        let finfo = FileInfoGDrive::from_object(f, &gdrive, &dmap).unwrap();
        assert_eq!(finfo.get_finfo().filename, "armstrong_thesis_2003.pdf");
        assert_eq!(
            finfo.get_finfo().serviceid.as_ref().unwrap().0.as_str(),
            "1M6EzRPGaJBaZgN_2bUQPcgKY2o7JXJvb"
        );
    }
}
