use failure::{err_msg, format_err, Error};
use log::debug;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::{create_dir_all, remove_file};
use std::io::{stdout, Write};
use std::path::Path;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

#[derive(Debug, Clone)]
pub struct FileListS3 {
    pub flist: FileList,
    pub s3: S3Instance,
}

#[derive(Debug, Clone)]
pub struct FileListS3Conf(pub FileListConf);

impl FileListS3 {
    pub fn from_conf(conf: FileListS3Conf) -> FileListS3 {
        let s3 = S3Instance::new(&conf.get_config().aws_region_name);

        FileListS3 {
            flist: FileList::from_conf(conf.0),
            s3,
        }
    }

    pub fn with_list(&self, filelist: Vec<FileInfo>) -> FileListS3 {
        FileListS3 {
            flist: self.flist.with_list(filelist),
            s3: self.s3.clone(),
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.s3 = self.s3.max_keys(max_keys);
        self
    }
}

impl FileListS3Conf {
    pub fn new(bucket: &str, config: &Config) -> Result<FileListS3Conf, Error> {
        let baseurl: Url = format!("s3://{}", bucket).parse()?;
        let basepath = Path::new("");

        let conf = FileListConf {
            baseurl,
            basepath: basepath.to_path_buf(),
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
            let basepath = Path::new(url.path());
            let bucket = url.host_str().ok_or_else(|| err_msg("Parse error"))?;
            let conf = FileListConf {
                baseurl: url.clone(),
                basepath: basepath.to_path_buf(),
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

        self.s3
            .get_list_of_keys(bucket, Some(prefix))?
            .into_par_iter()
            .map(|f| FileInfoS3::from_object(bucket, f).map(FileInfoTrait::into_finfo))
            .collect()
    }

    fn print_list(&self) -> Result<(), Error> {
        let conf = self.get_conf();
        let bucket = conf
            .baseurl
            .host_str()
            .ok_or_else(|| err_msg("Parse error"))?;
        let prefix = conf.baseurl.path().trim_start_matches('/');

        self.s3.process_list_of_keys(bucket, Some(prefix), |i| {
            writeln!(
                stdout().lock(),
                "s3://{}/{}",
                bucket,
                i.key.as_ref().map(String::as_str).unwrap_or_else(|| "")
            )?;
            Ok(())
        })
    }

    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::S3 && finfo1.servicetype == FileService::Local {
            let local_file = finfo1
                .filepath
                .clone()
                .ok_or_else(|| err_msg("No local path"))?
                .to_string_lossy()
                .to_string();
            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| err_msg("No local path"))?
                .parent()
                .ok_or_else(|| err_msg("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(&parent_dir)?;
            }
            let remote_url = finfo0.urlname.clone().ok_or_else(|| err_msg("No s3 url"))?;
            let bucket = remote_url.host_str().ok_or_else(|| err_msg("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            if Path::new(&local_file).exists() {
                remove_file(&local_file)?;
            }
            let md5sum = self.s3.download(&bucket, &key, &local_file)?;
            if md5sum
                != finfo1
                    .md5sum
                    .clone()
                    .map(|u| u.0)
                    .unwrap_or_else(|| "".to_string())
            {
                debug!(
                    "Multipart upload? {} {}",
                    finfo1
                        .urlname
                        .clone()
                        .map(Url::into_string)
                        .unwrap_or_else(|| "".to_string()),
                    finfo0
                        .urlname
                        .clone()
                        .map(Url::into_string)
                        .unwrap_or_else(|| "".to_string())
                );
            }
            Ok(())
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        }
    }

    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::S3 {
            let local_file = finfo0
                .filepath
                .clone()
                .ok_or_else(|| err_msg("No local path"))?
                .canonicalize()?
                .to_string_lossy()
                .to_string();
            let remote_url = finfo1.urlname.clone().ok_or_else(|| err_msg("No s3 url"))?;
            let bucket = remote_url.host_str().ok_or_else(|| err_msg("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            self.s3.upload(&local_file, &bucket, &key)
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        }
    }

    fn move_file<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype != finfo1.servicetype
            || self.get_conf().servicetype != finfo0.servicetype
        {
            return Ok(());
        }
        let url0 = finfo0.urlname.as_ref().ok_or_else(|| err_msg("No url"))?;
        let bucket0 = url0.host_str().ok_or_else(|| err_msg("Parse error"))?;
        let key0 = url0.path();
        let url1 = finfo1.urlname.as_ref().ok_or_else(|| err_msg("No url"))?;
        let bucket1 = url1.host_str().ok_or_else(|| err_msg("Parse error"))?;
        let key1 = url1.path();
        let new_tag = self.s3.copy_key(url0, &bucket1, &key1)?;
        if new_tag.is_some() {
            self.s3.delete_key(bucket0, key0)?;
        }
        Ok(())
    }

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait,
    {
        let finfo = finfo.get_finfo();
        if finfo.servicetype != FileService::S3 {
            Err(err_msg("Wrong service type"))
        } else {
            let url = finfo.urlname.clone().ok_or_else(|| err_msg("No s3 url"))?;
            let bucket = url.host_str().ok_or_else(|| err_msg("No bucket"))?;
            let key = url.path();
            self.s3.delete_key(&bucket, &key)
        }
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
        let config = Config::init_config().unwrap();
        let s3 = S3Instance::new(&config.aws_region_name);
        let blist = s3.get_list_of_buckets().unwrap();
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let conf = FileListS3Conf::new(&bucket, &config).unwrap();
        let flist = FileListS3::from_conf(conf).max_keys(100);

        let new_flist = flist.fill_file_list(None).unwrap();

        println!("{} {:?}", bucket, new_flist.get(0));
        assert!(new_flist.len() > 0);

        let config = Config::init_config().unwrap();
        let pool = PgPool::new(&config.database_url);
        let flist = flist.with_list(new_flist);

        flist.cache_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(flist.flist.filemap.len(), new_flist.len());

        flist.clear_file_list(&pool).unwrap();
    }
}
