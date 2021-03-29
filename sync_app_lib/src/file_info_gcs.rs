use anyhow::{format_err, Error};
use chrono::DateTime;
use std::path::Path;
use url::Url;

use gdrive_lib::{gcs_instance::GcsInstance, storage_v1_types::Object};

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Default, Clone)]
pub struct FileInfoGCS(FileInfo);

impl FileInfoGCS {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "gs" {
            return Err(format_err!("Invalid URL"));
        }
        let bucket = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key = url.path();
        let filepath = Path::new(&key);
        let filename = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_string_lossy()
            .into_owned()
            .into();
        let fileurl: Url = format!("gs://{}/{}", bucket, key).parse()?;
        let serviceid = bucket.to_string().into();
        let servicesession = bucket.parse()?;

        let finfo = FileInfo::new(
            filename,
            filepath.to_path_buf().into(),
            fileurl.into(),
            None,
            None,
            FileStat::default(),
            serviceid,
            FileService::GCS,
            servicesession,
        );
        Ok(Self(finfo))
    }
}

impl FileInfoTrait for FileInfoGCS {
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

    fn get_stat(&self) -> FileStat {
        self.0.filestat
    }
}

impl FileInfoGCS {
    pub fn from_object(bucket: &str, item: Object) -> Result<Self, Error> {
        let key = item.name.as_ref().ok_or_else(|| format_err!("No key"))?;
        let filepath = Path::new(&key);
        let filename = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_os_string()
            .to_string_lossy()
            .into_owned()
            .into();
        let md5sum = item.md5_hash.and_then(|m| m.trim_matches('"').parse().ok());
        let st_mtime = item
            .updated
            .as_ref()
            .ok_or_else(|| format_err!("No last modified"))?
            .timestamp();
        let size = item.size.ok_or_else(|| format_err!("No file size"))?;
        let st_size = size.parse()?;
        let fileurl: Url = format!("gs://{}/{}", bucket, key).parse()?;

        let serviceid = bucket.to_string().into();
        let servicesession = bucket.parse()?;

        let finfo = FileInfo::new(
            filename,
            filepath.to_path_buf().into(),
            fileurl.into(),
            md5sum,
            None,
            FileStat {
                st_mtime: st_mtime as u32,
                st_size,
            },
            serviceid,
            FileService::GCS,
            servicesession,
        );

        Ok(Self(finfo))
    }
}

#[cfg(test)]
mod tests {
    use gdrive_lib::storage_v1_types::{Object, ObjectOwner};

    use crate::{file_info::FileInfoTrait, file_info_gcs::FileInfoGCS};

    #[test]
    fn test_file_info_gcs() {
        let test_owner = ObjectOwner {
            entity: Some("me".to_string()),
            entity_id: Some("8675309".to_string()),
        };
        let test_object = Object {
            etag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.into()),
            name: Some("test_key".into()),
            updated: "2019-05-01T00:00:00+00:00".parse().ok(),
            owner: Some(test_owner),
            size: Some("100".into()),
            storage_class: Some("Standard".into()),
            ..Object::default()
        };

        let finfo = FileInfoGCS::from_object("test_bucket", test_object).unwrap();

        assert_eq!(
            finfo.get_finfo().urlname.as_str(),
            "gs://test_bucket/test_key"
        );
        assert_eq!(&finfo.get_finfo().filename, "test_key");
    }
}
