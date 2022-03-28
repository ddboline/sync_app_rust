use anyhow::{format_err, Error};
use chrono::DateTime;
use rusoto_s3::Object;
use stack_string::{format_sstr, StackString};
use std::path::Path;
use url::Url;

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Default, Clone)]
pub struct FileInfoS3(FileInfo);

impl FileInfoS3 {
    /// # Errors
    /// Return error if init fails
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "s3" {
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
        let fileurl = format_sstr!("s3://{bucket}/{key}");
        let fileurl: Url = fileurl.parse()?;
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
            FileService::S3,
            servicesession,
        );
        Ok(Self(finfo))
    }
}

impl FileInfoTrait for FileInfoS3 {
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

impl FileInfoS3 {
    /// # Errors
    /// Return error if init fails
    pub fn from_object(bucket: &str, item: Object) -> Result<Self, Error> {
        let key = item.key.as_ref().ok_or_else(|| format_err!("No key"))?;
        let filepath = Path::new(&key);
        let filename = filepath
            .file_name()
            .ok_or_else(|| format_err!("Parse failure"))?
            .to_os_string()
            .to_string_lossy()
            .into_owned()
            .into();
        let md5sum = item.e_tag.and_then(|m| m.trim_matches('"').parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.last_modified
                .as_ref()
                .ok_or_else(|| format_err!("No last modified"))?,
        )?
        .timestamp();
        let size = item.size.ok_or_else(|| format_err!("No file size"))?;
        let fileurl = format_sstr!("s3://{bucket}/{key}");
        let fileurl: Url = fileurl.parse()?;
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
                st_size: size as u32,
            },
            serviceid,
            FileService::S3,
            servicesession,
        );

        Ok(Self(finfo))
    }
}

#[cfg(test)]
mod tests {
    use rusoto_s3::{Object, Owner};

    use crate::{file_info::FileInfoTrait, file_info_s3::FileInfoS3};

    #[test]
    fn test_file_info_s3() {
        let test_owner = Owner {
            display_name: Some("me".to_string()),
            id: Some("8675309".to_string()),
        };
        let test_object = Object {
            e_tag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.to_string()),
            key: Some("test_key".to_string()),
            last_modified: Some("2019-05-01T00:00:00+00:00".to_string()),
            owner: Some(test_owner),
            size: Some(100),
            storage_class: Some("Standard".to_string()),
        };

        let finfo = FileInfoS3::from_object("test_bucket", test_object).unwrap();

        assert_eq!(
            finfo.get_finfo().urlname.as_str(),
            "s3://test_bucket/test_key"
        );
        assert_eq!(&finfo.get_finfo().filename, "test_key");
    }
}
