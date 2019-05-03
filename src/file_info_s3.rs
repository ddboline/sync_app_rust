use chrono::DateTime;
use failure::{err_msg, Error};
use rusoto_s3::{Bucket, Object};

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, ServiceId, Sha1Sum};
use crate::file_service::FileService;

pub struct FileInfoS3(pub FileInfo);

impl FileInfoTrait for FileInfoS3 {
    fn get_md5(&self) -> Option<Md5Sum> {
        self.0.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.0.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.0.filestat.clone()
    }

    fn get_service_id(&self) -> Option<ServiceId> {
        self.0.serviceid.clone()
    }
}

impl FileInfoS3 {
    pub fn from_object(bucket: &str, item: Object) -> Result<FileInfoS3, Error> {
        let key = item.key.as_ref().ok_or_else(|| err_msg("No key"))?.clone();
        let md5sum = item.e_tag.clone().and_then(|m| m.parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.last_modified
                .as_ref()
                .ok_or_else(|| err_msg("No last modified"))?,
        )?
        .timestamp();
        let size = item.size.ok_or_else(|| err_msg("No file size"))?;
        let fileurl = format!("s3://{}/{}", bucket, key).parse()?;
        let serviceid = Some(bucket.to_string().into());
        let servicesession = Some(bucket.parse()?);

        let finfo = FileInfo {
            filename: key,
            filepath: None,
            urlname: Some(fileurl),
            md5sum,
            sha1sum: None,
            filestat: Some(FileStat {
                st_mtime: st_mtime as u32,
                st_size: size as u32,
            }),
            serviceid,
            servicetype: FileService::S3,
            servicesession,
        };

        Ok(FileInfoS3(finfo))
    }
}

#[cfg(test)]
mod tests {
    use rusoto_s3::{Bucket, Object, Owner};

    use crate::file_info_s3::FileInfoS3;

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
            finfo.0.urlname.unwrap().as_str(),
            "s3://test_bucket/test_key"
        );
        assert_eq!(&finfo.0.filename, "test_key");
    }
}
