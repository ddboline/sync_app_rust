use anyhow::{format_err, Error};
use std::path::Path;
use url::Url;

use gdrive_lib::gdrive_instance::GDriveInfo;
use stack_string::StackString;

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Default)]
pub struct FileInfoGDrive(pub FileInfo);

impl FileInfoGDrive {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "gdrive" {
            return Err(format_err!("Invalid URL"));
        }
        let path = url.path();
        let filepath = Path::new(&path);
        let filename: StackString = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_os_string()
            .to_string_lossy()
            .into_owned()
            .into();
        let serviceid = Some(filename.clone().into());
        let servicesession = url
            .as_str()
            .trim_start_matches("gdrive://")
            .replace(url.path(), "")
            .parse()?;

        let finfo = FileInfo::new(
            filename,
            Some(filepath.to_path_buf().into()),
            Some(url.clone().into()),
            None,
            None,
            None,
            serviceid,
            FileService::GDrive,
            Some(servicesession),
        );
        Ok(Self(finfo))
    }
}

impl FileInfoTrait for FileInfoGDrive {
    fn get_finfo(&self) -> &FileInfo {
        &self.0
    }

    fn into_finfo(self) -> FileInfo {
        self.0
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
    pub fn from_gdriveinfo(item: GDriveInfo) -> Result<Self, Error> {
        let md5sum = item.md5sum.and_then(|m| m.parse().ok());
        let serviceid = item.serviceid.map(|x| x.into());
        let servicesession = item.servicesession.and_then(|s| s.parse().ok());

        let finfo = FileInfo::new(
            item.filename.into(),
            item.filepath.map(Into::into),
            item.urlname.map(Into::into),
            md5sum,
            None,
            item.filestat.map(|i| FileStat {
                st_mtime: i.0,
                st_size: i.1,
            }),
            serviceid,
            FileService::GDrive,
            servicesession,
        );

        Ok(Self(finfo))
    }
}

#[cfg(test)]
mod tests {
    use log::debug;
    use std::{collections::HashMap, path::Path};
    use url::Url;

    use gdrive_lib::gdrive_instance::{GDriveInfo, GDriveInstance};

    use crate::{
        config::Config, file_info::FileInfoTrait, file_info_gdrive::FileInfoGDrive,
        file_service::FileService,
    };

    #[test]
    fn test_file_info_gdrive() {
        let url: Url = "gdrive://user@domain.com/My Drive/test.txt"
            .parse()
            .unwrap();
        let finfo = FileInfoGDrive::from_url(&url).unwrap();
        debug!("{:?}", finfo);
        assert_eq!(finfo.get_finfo().filename, "test.txt");
        assert_eq!(finfo.get_finfo().servicetype, FileService::GDrive);
    }

    #[test]
    #[ignore]
    fn test_file_info_from_object() {
        let f = GDriveInfo {
            filename: "armstrong_thesis_2003.pdf".into(),
            filepath: Some("armstrong_thesis_2003.pdf".parse().unwrap()),
            urlname: Some(
                "gdrive://ddboline@gmail.com/My%20Drive/armstrong_thesis_2003.pdf"
                    .parse()
                    .unwrap(),
            ),
            md5sum: Some("afde42b3861d522796faeb33a9eaec8a".into()),
            sha1sum: None,
            filestat: Some((123, 123)),
            serviceid: Some("1REd76oJ6YheyjF2R9Il0E8xbjalgpNgG".into()),
            servicesession: Some("ddboline@gmail.com".into()),
        };

        let finfo = FileInfoGDrive::from_gdriveinfo(f).unwrap();
        assert_eq!(finfo.get_finfo().filename, "armstrong_thesis_2003.pdf");
        assert_eq!(
            finfo.get_finfo().serviceid.as_ref().unwrap().0.as_str(),
            "1REd76oJ6YheyjF2R9Il0E8xbjalgpNgG"
        );
    }

    #[test]
    #[ignore]
    fn test_create_drive() {
        let config = Config::init_config().unwrap();
        let gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            "ddboline@gmail.com",
        )
        .with_max_keys(10)
        .with_page_size(10)
        .read_start_page_token_from_file();

        let list = gdrive.get_all_files(false).unwrap();
        assert_eq!(list.len(), 10);
        let test_info = list.iter().filter(|f| !f.parents.is_none()).next().unwrap();
        debug!("test_info {:?}", test_info);

        let gdriveid = test_info.id.as_ref().unwrap();
        let parent = &test_info.parents.as_ref().unwrap()[0];
        let local_path = Path::new("/tmp/temp.file");
        let mime = test_info.mime_type.as_ref().unwrap().to_string();
        debug!("mime {}", mime);
        gdrive
            .download(&gdriveid, &local_path, &Some(mime))
            .unwrap();

        let basepath = Path::new("../gdrive_lib/src/gdrive_instance.rs")
            .canonicalize()
            .unwrap();
        let local_url = Url::from_file_path(basepath).unwrap();
        let new_file = gdrive.upload(&local_url, &parent).unwrap();
        debug!("new_file {:?}", new_file);
        debug!("start_page_token {:?}", gdrive.start_page_token);
        debug!(
            "current_start_page_token {:?}",
            gdrive.get_start_page_token().unwrap()
        );

        let changes = gdrive.get_all_changes().unwrap();
        let changes_map: HashMap<_, _> = changes
            .into_iter()
            .filter_map(|c| {
                if let Some((t, f)) = c
                    .file_id
                    .as_ref()
                    .and_then(|f| c.time.as_ref().and_then(|t| Some((f.clone(), t.clone()))))
                {
                    Some(((t, f), c))
                } else {
                    None
                }
            })
            .collect();
        debug!("N files {}", changes_map.len());
        for ((f, t), ch) in changes_map {
            debug!("ch {:?} {:?} {:?}", f, t, ch.file);
        }

        let new_driveid = new_file.id.unwrap();
        gdrive.move_to_trash(&new_driveid).unwrap();
        debug!(
            "trash {:?}",
            gdrive.get_file_metadata(&new_driveid).unwrap()
        );

        gdrive.delete_permanently(&new_driveid).unwrap();
        debug!(
            "error {}",
            gdrive.get_file_metadata(&new_driveid).unwrap_err()
        );
    }

    #[test]
    #[ignore]
    fn test_gdrive_store_read_change_token() {
        let config = Config::init_config().unwrap();
        let gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            "ddboline@gmail.com",
        )
        .with_max_keys(10)
        .with_page_size(10)
        .with_start_page_token("test_string");
        let p = Path::new("/tmp/temp_start_page_token.txt");
        gdrive.store_start_page_token(&p).unwrap();
        let result = GDriveInstance::read_start_page_token(&p).unwrap();
        assert_eq!(result, Some("test_string".into()));
    }
}
