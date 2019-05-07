use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::collections::HashMap;
use std::env::var;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use crate::file_info::FileInfo;
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListConf, FileListTrait};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

#[derive(Debug, Clone)]
pub struct FileListS3 {
    pub flist: FileList,
    pub s3: Arc<S3Instance>,
    pub basedir: String,
}

impl FileListS3 {
    pub fn from_conf(
        conf: FileListConf,
        region_name: Option<&str>,
        basedir: Option<&str>,
    ) -> FileListS3 {
        let s3 = Arc::new(S3Instance::new(region_name));
        let basedir = basedir
            .and_then(|s| {
                if Path::new(&s).exists() {
                    Some(s.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| format!("{}/S3", var("HOME").unwrap_or_else(|_| "/".to_string())));

        FileListS3 {
            flist: FileList::from_conf(conf),
            s3,
            basedir,
        }
    }

    pub fn with_list(&self, filelist: &[FileInfo]) -> FileListS3 {
        FileListS3 {
            flist: self.flist.with_list(&filelist),
            s3: self.s3.clone(),
            basedir: self.basedir.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileListS3Conf(pub FileListConf);

impl FileListS3Conf {
    pub fn new(bucket: &str) -> Result<FileListS3Conf, Error> {
        let baseurl: Url = format!("s3://{}", bucket).parse()?;

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

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.flist.filemap
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

    fn upload_file(&self, finfo_local: &FileInfo, finfo_remote: &FileInfo) -> Result<(), Error> {
        if finfo_local.servicetype != FileService::Local
            || finfo_remote.servicetype != FileService::S3
        {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        let local_file = finfo_local
            .filepath
            .clone()
            .ok_or_else(|| err_msg("No local path"))?
            .canonicalize()?
            .to_str()
            .ok_or_else(|| err_msg("Failed to parse path"))?
            .to_string();
        let remote_url = finfo_remote
            .urlname
            .clone()
            .ok_or_else(|| err_msg("No s3 url"))?;
        let bucket = remote_url.host_str().ok_or_else(|| err_msg("No bucket"))?;
        let key = remote_url.path();
        self.s3.upload(&local_file, &bucket, &key)
    }

    fn download_file(&self, finfo_remote: &FileInfo, finfo_local: &FileInfo) -> Result<(), Error> {
        if finfo_local.servicetype != FileService::Local
            || finfo_remote.servicetype != FileService::S3
        {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        let local_file = finfo_local
            .filepath
            .clone()
            .ok_or_else(|| err_msg("No local path"))?
            .canonicalize()?
            .to_str()
            .ok_or_else(|| err_msg("Failed to parse path"))?
            .to_string();
        let parent_dir = finfo_local
            .filepath
            .as_ref()
            .ok_or_else(|| err_msg("No local path"))?
            .parent()
            .ok_or_else(|| err_msg("No parent directory"))?;
        if !parent_dir.exists() {
            create_dir_all(&parent_dir)?;
        }
        let remote_url = finfo_remote
            .urlname
            .clone()
            .ok_or_else(|| err_msg("No s3 url"))?;
        let bucket = remote_url.host_str().ok_or_else(|| err_msg("No bucket"))?;
        let key = remote_url.path();
        let md5sum = self.s3.download(&bucket, &key, &local_file)?;
        if md5sum
            != finfo_local
                .md5sum
                .clone()
                .map(|u| u.0)
                .unwrap_or_else(|| "".to_string())
        {
            println!(
                "Multipart upload? {} {}",
                finfo_local
                    .urlname
                    .clone()
                    .map(Url::into_string)
                    .unwrap_or_else(|| "".to_string()),
                finfo_remote
                    .urlname
                    .clone()
                    .map(Url::into_string)
                    .unwrap_or_else(|| "".to_string())
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::file_list::FileListTrait;
    use crate::file_list_s3::{FileListS3, FileListS3Conf};
    use crate::pgpool::PgPool;
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
        let flist = FileListS3::from_conf(conf.0, None, None);

        let new_flist = flist.fill_file_list(None).unwrap();

        println!("{} {:?}", bucket, new_flist.get(0));
        assert!(new_flist.len() > 0);

        let config = Config::new();
        let pool = PgPool::new(&config.database_url);
        let flist = flist.with_list(&new_flist);

        flist.cache_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(flist.flist.filemap.len(), new_flist.len());

        flist.clear_file_list(&pool).unwrap();
    }
}
