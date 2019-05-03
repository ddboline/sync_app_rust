use chrono::DateTime;
use failure::{err_msg, Error};
use rusoto_s3::{Bucket, Object};

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, ServiceId, Sha1Sum};
use crate::file_service::FileService;

pub struct FileInfoS3 {
    pub finfo: FileInfo,
    pub bucket: Bucket,
}

impl FileInfoTrait for FileInfoS3 {
    fn get_md5(&self) -> Option<Md5Sum> {
        self.finfo.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.finfo.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.finfo.filestat.clone()
    }

    fn get_service_id(&self) -> Option<ServiceId> {
        self.finfo.serviceid.clone()
    }
}

impl FileInfoS3 {
    pub fn from_object(bucket: Bucket, item: Object) -> Result<FileInfoS3, Error> {
        let bucket_name = bucket.clone().name.ok_or_else(|| err_msg("No bucket"))?;
        let key = item.key.as_ref().ok_or_else(|| err_msg("No key"))?.clone();
        let md5sum = item.e_tag.clone().and_then(|m| m.parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.last_modified
                .as_ref()
                .ok_or_else(|| err_msg("No last modified"))?,
        )?
        .timestamp();
        let size = item.size.ok_or_else(|| err_msg("No file size"))?;
        let fileurl = format!("s3://{}/{}", bucket_name, key).parse()?;
        let serviceid = Some(bucket_name.clone().into());
        let servicesession = Some(bucket_name.parse()?);

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
            servicetype: FileService::Local,
            servicesession,
        };

        Ok(FileInfoS3 { finfo, bucket })
    }
}
