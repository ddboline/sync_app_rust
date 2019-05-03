use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::collections::HashMap;
use std::sync::Arc;

use crate::file_info::FileInfo;
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListConf, FileListTrait};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

pub struct FileListS3 {
    pub flist: FileList,
    pub s3: Arc<S3Instance>,
}

impl FileListS3 {
    pub fn from_conf(region_name: Option<&str>, conf: FileListConf) -> FileListS3 {
        let s3 = Arc::new(S3Instance::new(region_name));

        FileListS3 {
            flist: FileList::from_conf(conf),
            s3,
        }
    }
}

pub struct FileListS3Conf(pub FileListConf);

impl FileListS3Conf {
    pub fn new(bucket: &str) -> Result<FileListS3Conf, Error> {
        let baseurl: Url = format!("s3://{}/", bucket).parse()?;

        let conf = FileListConf {
            baseurl,
            servicetype: FileService::S3,
            servicesession: bucket.parse()?,
            serviceid: bucket.to_string().into(),
        };
        Ok(FileListS3Conf(conf))
    }
}

impl FileListTrait for FileListS3 {
    fn get_conf(&self) -> &FileListConf {
        &self.flist.conf
    }

    fn get_filelist(&self) -> &[FileInfo] {
        &self.flist.filelist
    }

    fn fill_file_list(&self, _: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();
        let bucket = conf
            .baseurl
            .host_str()
            .ok_or_else(|| err_msg("Parse error"))?;

        let flist: Vec<Result<_, Error>> = self
            .s3
            .get_list_of_keys(bucket)?
            .into_par_iter()
            .map(|f| FileInfoS3::from_object(bucket, f).map(|i| i.0))
            .collect();
        let flist = map_result_vec(flist)?;

        Ok(flist)
    }
}

#[cfg(test)]
mod tests {
    use crate::file_list_s3::{FileListS3, FileListS3Conf, FileListTrait};
    use crate::s3_instance::S3Instance;

    #[test]
    fn test_fill_file_list() {
        let s3 = S3Instance::new(None);
        let blist = s3.get_list_of_buckets().unwrap();
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let conf = FileListS3Conf::new(&bucket).unwrap();
        let flist = FileListS3::from_conf(None, conf.0);

        let new_flist = flist.fill_file_list(None).unwrap();

        println!("{:?}", new_flist.len());
        assert_eq!(new_flist.len(), 1000);
    }
}
