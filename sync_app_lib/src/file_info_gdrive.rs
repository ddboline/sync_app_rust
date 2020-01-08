use anyhow::{format_err, Error};
use std::path::Path;
use url::Url;

use gdrive_lib::gdrive_instance::GDriveInfo;

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum};
use crate::file_service::FileService;

#[derive(Debug, Default)]
pub struct FileInfoGDrive(pub FileInfo);

impl FileInfoTrait for FileInfoGDrive {
    fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "gdrive" {
            return Err(format_err!("Invalid URL"));
        }
        let path = url.path();
        let filepath = Path::new(&path);
        let filename = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_os_string()
            .into_string()
            .map_err(|_| format_err!("Parse failure"))?;
        let serviceid = Some(filename.to_string().into());
        let servicesession = url
            .as_str()
            .trim_start_matches("gdrive://")
            .replace(url.path(), "")
            .parse()?;

        let finfo = FileInfo {
            filename,
            filepath: Some(filepath.to_path_buf()),
            urlname: Some(url.clone()),
            md5sum: None,
            sha1sum: None,
            filestat: None,
            serviceid,
            servicetype: FileService::GDrive,
            servicesession: Some(servicesession),
        };
        Ok(Self(finfo))
    }

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

        let finfo = FileInfo {
            filename: item.filename,
            filepath: item.filepath,
            urlname: item.urlname,
            md5sum,
            sha1sum: None,
            filestat: item.filestat.map(|i| FileStat {
                st_mtime: i.0,
                st_size: i.1,
            }),
            serviceid,
            servicetype: FileService::GDrive,
            servicesession,
        };

        Ok(Self(finfo))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{stdout, Write};
    use std::path::Path;
    use url::Url;

    use gdrive_lib::gdrive_instance::{GDriveInfo, GDriveInstance};

    use crate::config::Config;
    use crate::file_info::FileInfoTrait;
    use crate::file_info_gdrive::FileInfoGDrive;
    use crate::file_service::FileService;

    #[test]
    fn test_file_info_gdrive() {
        let url: Url = "gdrive://user@domain.com/My Drive/test.txt"
            .parse()
            .unwrap();
        let finfo = FileInfoGDrive::from_url(&url).unwrap();
        writeln!(stdout(), "{:?}", finfo).unwrap();
        assert_eq!(finfo.get_finfo().filename, "test.txt");
        assert_eq!(finfo.get_finfo().servicetype, FileService::GDrive);
    }

    #[test]
    #[ignore]
    fn test_file_info_from_object() {
        let f = GDriveInfo {
            filename: "armstrong_thesis_2003.pdf".to_string(),
            filepath: Some("armstrong_thesis_2003.pdf".parse().unwrap()),
            urlname: Some(
                "gdrive://ddboline@gmail.com/My%20Drive/armstrong_thesis_2003.pdf"
                    .parse()
                    .unwrap(),
            ),
            md5sum: Some("afde42b3861d522796faeb33a9eaec8a".to_string()),
            sha1sum: None,
            filestat: Some((123, 123)),
            serviceid: Some("1REd76oJ6YheyjF2R9Il0E8xbjalgpNgG".to_string()),
            servicesession: Some("ddboline@gmail.com".to_string()),
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
        let test_info = list.iter().filter(|f| !f.parents.is_none()).nth(0).unwrap();
        writeln!(stdout(), "test_info {:?}", test_info).unwrap();

        let gdriveid = test_info.id.as_ref().unwrap();
        let parent = &test_info.parents.as_ref().unwrap()[0];
        let local_path = Path::new("/tmp/temp.file");
        let mime = test_info.mime_type.as_ref().unwrap().to_string();
        writeln!(stdout(), "mime {}", mime).unwrap();
        gdrive
            .download(&gdriveid, &local_path, &Some(mime))
            .unwrap();

        let basepath = Path::new("../gdrive_lib/src/gdrive_instance.rs")
            .canonicalize()
            .unwrap();
        let local_url = Url::from_file_path(basepath).unwrap();
        let new_file = gdrive.upload(&local_url, &parent).unwrap();
        writeln!(stdout(), "new_file {:?}", new_file).unwrap();
        writeln!(stdout(), "start_page_token {:?}", gdrive.start_page_token).unwrap();
        writeln!(
            stdout(),
            "current_start_page_token {:?}",
            gdrive.get_start_page_token().unwrap()
        )
        .unwrap();

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
        writeln!(stdout(), "N files {}", changes_map.len()).unwrap();
        for ((f, t), ch) in changes_map {
            writeln!(stdout(), "ch {:?} {:?} {:?}", f, t, ch.file).unwrap();
        }

        let new_driveid = new_file.id.unwrap();
        gdrive.move_to_trash(&new_driveid).unwrap();
        writeln!(
            stdout(),
            "trash {:?}",
            gdrive.get_file_metadata(&new_driveid).unwrap()
        )
        .unwrap();

        gdrive.delete_permanently(&new_driveid).unwrap();
        writeln!(
            stdout(),
            "error {}",
            gdrive.get_file_metadata(&new_driveid).unwrap_err()
        )
        .unwrap();
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
        assert_eq!(result, Some("test_string".to_string()));
    }
}
