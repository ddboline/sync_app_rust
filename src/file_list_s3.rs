use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use rusoto_s3::{ListObjectsV2Request, Object};
use std::collections::HashMap;

use crate::file_info::FileInfo;
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListConf, FileListTrait};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

pub struct FileListS3(pub FileList);

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
        &self.0.conf
    }

    fn get_filelist(&self) -> &[FileInfo] {
        &self.0.filelist
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();
        let flist_dict = match pool {
            Some(pool) => self.get_file_list_dict(&pool)?,
            None => HashMap::new(),
        };
        let s3_instance = S3Instance::new(None);

        let bucket = conf
            .baseurl
            .host_str()
            .ok_or_else(|| err_msg("Parse error"))?;

        let flist: Vec<Result<_, Error>> = s3_instance
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
    use rusoto_s3::{Bucket, Object, Owner};

    use crate::file_list::FileList;
    use crate::file_list_s3::{FileListS3, FileListS3Conf, FileListTrait};

    #[test]
    fn test_fill_file_list() {
        let conf = FileListS3Conf::new("appstorage.dev.fpgv3").unwrap();
        let flist = FileList {
            conf: conf.0,
            filelist: Vec::new(),
        };
        let flist = FileListS3(flist);

        let new_flist = flist.fill_file_list(None).unwrap();

        print!("{:?}", new_flist);
        assert!(false);
    }
}
