use failure::{err_msg, Error};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::create_dir_all;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::map_result_vec;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

#[derive(Debug, Clone)]
pub struct FileListS3 {
    pub flist: FileList,
    pub s3: S3Instance,
}

impl FileListS3 {
    pub fn from_conf(conf: FileListS3Conf, region_name: Option<&str>) -> FileListS3 {
        let s3 = S3Instance::new(region_name);

        FileListS3 {
            flist: FileList::from_conf(conf.0),
            s3,
        }
    }

    pub fn with_list(&self, filelist: &[FileInfo]) -> FileListS3 {
        FileListS3 {
            flist: self.flist.with_list(&filelist),
            s3: self.s3.clone(),
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.s3 = self.s3.max_keys(max_keys);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FileListS3Conf(pub FileListConf);

impl FileListS3Conf {
    pub fn new(bucket: &str, config: &Config) -> Result<FileListS3Conf, Error> {
        let baseurl: Url = format!("s3://{}", bucket).parse()?;

        let conf = FileListConf {
            baseurl,
            config: config.clone(),
            servicetype: FileService::S3,
            servicesession: bucket.parse()?,
        };
        Ok(FileListS3Conf(conf))
    }
}

impl FileListConfTrait for FileListS3Conf {
    fn from_url(url: &Url, config: &Config) -> Result<FileListS3Conf, Error> {
        if url.scheme() != "s3" {
            Err(err_msg("Wrong scheme"))
        } else {
            let bucket = url.host_str().ok_or_else(|| err_msg("Parse error"))?;
            let conf = FileListConf {
                baseurl: url.clone(),
                config: config.clone(),
                servicetype: FileService::S3,
                servicesession: bucket.parse()?,
            };

            Ok(FileListS3Conf(conf))
        }
    }

    fn get_config(&self) -> &Config {
        &self.0.config
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
        let prefix = conf.baseurl.path().trim_start_matches('/');

        let flist: Vec<Result<_, Error>> = self
            .s3
            .get_list_of_keys(bucket, Some(prefix))?
            .into_par_iter()
            .map(|f| FileInfoS3::from_object(bucket, f).map(|i| i.0))
            .collect();
        let flist = map_result_vec(flist)?;

        Ok(flist)
    }

    fn print_list(&self) -> Result<(), Error> {
        let conf = self.get_conf();
        let bucket = conf
            .baseurl
            .host_str()
            .ok_or_else(|| err_msg("Parse error"))?;
        let prefix = conf.baseurl.path().trim_start_matches('/');

        self.s3.process_list_of_keys(bucket, Some(prefix), |i| {
            println!(
                "s3://{}/{}",
                bucket,
                i.key.as_ref().map(String::as_str).unwrap_or_else(|| "")
            );
        })
    }

    fn upload_file<T, U>(&self, finfo_local: &T, finfo_remote: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_local = finfo_local.get_finfo();
        let finfo_remote = finfo_remote.get_finfo();
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

    fn download_file<T, U>(&self, finfo_remote: &T, finfo_local: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_remote = finfo_remote.get_finfo();
        let finfo_local = finfo_local.get_finfo();
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
        let key = remote_url.path().trim_start_matches('/');
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
        let config = Config::new();

        let conf = FileListS3Conf::new(&bucket, &config).unwrap();
        let flist = FileListS3::from_conf(conf, None).max_keys(100);

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
