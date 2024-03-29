use anyhow::{format_err, Error};
use stack_string::{format_sstr, StackString};
use std::path::Path;
use url::Url;

use gdrive_lib::storage_v1_types::Object;

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Default, Clone)]
pub struct FileInfoGcs(FileInfo);

impl FileInfoGcs {
    /// # Errors
    /// Return error if init fails
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "gs" {
            return Err(format_err!("Invalid URL"));
        }
        let bucket: StackString = url
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?
            .into();
        let key = url.path();
        let filepath = Path::new(&key);
        let filename = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_string_lossy()
            .into_owned()
            .into();
        let buf = format_sstr!("gs://{bucket}/{key}");
        let fileurl: Url = buf.parse()?;
        let serviceid = bucket.clone().into();
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

impl FileInfoTrait for FileInfoGcs {
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

impl FileInfoGcs {
    /// # Errors
    /// Return error if init fails
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
            .unix_timestamp();
        let size = item.size.ok_or_else(|| format_err!("No file size"))?;
        let st_size = size.parse()?;
        let buf = format_sstr!("gs://{bucket}/{key}");
        let fileurl: Url = buf.parse()?;
        let id_str: StackString = bucket.into();
        let serviceid = id_str.into();
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
    use time::{macros::datetime, UtcOffset};

    use crate::{file_info::FileInfoTrait, file_info_gcs::FileInfoGcs};

    #[test]
    fn test_file_info_gcs() {
        let test_owner = ObjectOwner {
            entity: Some("me".to_string()),
            entity_id: Some("8675309".to_string()),
        };
        let test_object = Object {
            etag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.into()),
            name: Some("test_key".into()),
            updated: Some(
                datetime!(2019-05-01 00:00:00)
                    .assume_offset(UtcOffset::UTC)
                    .into(),
            ),
            owner: Some(test_owner),
            size: Some("100".into()),
            storage_class: Some("Standard".into()),
            ..Object::default()
        };

        let finfo = FileInfoGcs::from_object("test_bucket", test_object).unwrap();

        assert_eq!(
            finfo.get_finfo().urlname.as_str(),
            "gs://test_bucket/test_key"
        );
        assert_eq!(&finfo.get_finfo().filename, "test_key");
    }
}
