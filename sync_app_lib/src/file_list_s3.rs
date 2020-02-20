use anyhow::{format_err, Error};
use async_trait::async_trait;
use log::debug;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::{create_dir_all, remove_file};
use std::io::{stdout, Write};
use std::path::Path;
use tokio::task::spawn_blocking;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait, ServiceSession};
use crate::file_info_s3::FileInfoS3;
use crate::file_list::{FileList, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;
use crate::s3_instance::S3Instance;

#[derive(Debug, Clone)]
pub struct FileListS3 {
    pub flist: FileList,
    pub s3: S3Instance,
}

impl FileListS3 {
    pub fn new(bucket: &str, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let baseurl: Url = format!("s3://{}", bucket).parse()?;
        let basepath = Path::new("");

        let flist = FileList::new(
            baseurl,
            basepath.to_path_buf(),
            config.clone(),
            FileService::S3,
            bucket.parse()?,
            HashMap::new(),
            pool.clone(),
        );
        let s3 = S3Instance::new(&config.aws_region_name);

        Ok(Self { flist, s3 })
    }

    pub async fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "s3" {
            let basepath = Path::new(url.path());
            let bucket = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
            let flist = FileList::new(
                url.clone(),
                basepath.to_path_buf(),
                config.clone(),
                FileService::S3,
                bucket.parse()?,
                HashMap::new(),
                pool.clone(),
            );
            let config = config.clone();
            let s3 = spawn_blocking(move || S3Instance::new(&config.aws_region_name)).await?;

            Ok(Self { flist, s3 })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.s3 = self.s3.max_keys(max_keys);
        self
    }
}

#[async_trait]
impl FileListTrait for FileListS3 {
    fn get_baseurl(&self) -> &Url {
        self.flist.get_baseurl()
    }
    fn set_baseurl(&mut self, baseurl: Url) {
        self.flist.set_baseurl(baseurl);
    }
    fn get_basepath(&self) -> &Path {
        &self.flist.basepath
    }
    fn get_servicetype(&self) -> FileService {
        self.flist.servicetype
    }
    fn get_servicesession(&self) -> &ServiceSession {
        &self.flist.servicesession
    }
    fn get_config(&self) -> &Config {
        &self.flist.config
    }

    fn get_pool(&self) -> &PgPool {
        &self.flist.pool
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        self.flist.get_filemap()
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.flist.with_list(filelist)
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');

        self.s3
            .get_list_of_keys(bucket, Some(prefix))
            .await?
            .into_par_iter()
            .map(|f| FileInfoS3::from_object(bucket, f).map(FileInfoTrait::into_finfo))
            .collect()
    }

    async fn print_list(&self) -> Result<(), Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');

        self.s3
            .process_list_of_keys(bucket, Some(prefix), |i| {
                writeln!(
                    stdout().lock(),
                    "s3://{}/{}",
                    bucket,
                    i.key.as_ref().map_or_else(|| "", String::as_str)
                )?;
                Ok(())
            })
            .await
    }

    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::S3 && finfo1.servicetype == FileService::Local {
            let local_file = finfo1
                .filepath
                .clone()
                .ok_or_else(|| format_err!("No local path"))?
                .to_string_lossy()
                .to_string();
            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(&parent_dir)?;
            }
            let remote_url = finfo0
                .urlname
                .clone()
                .ok_or_else(|| format_err!("No s3 url"))?;
            let bucket = remote_url
                .host_str()
                .ok_or_else(|| format_err!("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            if Path::new(&local_file).exists() {
                remove_file(&local_file)?;
            }
            let md5sum = self.s3.download(&bucket, &key, &local_file).await?;
            if md5sum
                != finfo1
                    .md5sum
                    .clone()
                    .map_or_else(|| "".to_string(), |u| u.0)
            {
                debug!(
                    "Multipart upload? {} {}",
                    finfo1.urlname.as_ref().map_or_else(|| "", |u| u.as_str()),
                    finfo0.urlname.as_ref().map_or_else(|| "", |u| u.as_str()),
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

    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::S3 {
            let local_file = finfo0
                .filepath
                .clone()
                .ok_or_else(|| format_err!("No local path"))?
                .canonicalize()?
                .to_string_lossy()
                .to_string();
            let remote_url = finfo1
                .urlname
                .clone()
                .ok_or_else(|| format_err!("No s3 url"))?;
            let bucket = remote_url
                .host_str()
                .ok_or_else(|| format_err!("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            self.s3.upload(&local_file, &bucket, &key).await
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        }
    }

    async fn move_file(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype != finfo1.servicetype || self.get_servicetype() != finfo0.servicetype
        {
            return Ok(());
        }
        let url0 = finfo0
            .urlname
            .as_ref()
            .ok_or_else(|| format_err!("No url"))?;
        let bucket0 = url0.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key0 = url0.path();
        let url1 = finfo1
            .urlname
            .as_ref()
            .ok_or_else(|| format_err!("No url"))?;
        let bucket1 = url1.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key1 = url1.path();
        let new_tag = self.s3.copy_key(url0, &bucket1, &key1).await?;
        if new_tag.is_some() {
            self.s3.delete_key(bucket0, key0).await?;
        }
        Ok(())
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        if finfo.servicetype == FileService::S3 {
            let url = finfo
                .urlname
                .clone()
                .ok_or_else(|| format_err!("No s3 url"))?;
            let bucket = url.host_str().ok_or_else(|| format_err!("No bucket"))?;
            let key = url.path();
            self.s3.delete_key(&bucket, &key).await
        } else {
            Err(format_err!("Wrong service type"))
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use std::io::{stdout, Write};

    use crate::config::Config;
    use crate::file_list::FileListTrait;
    use crate::file_list_s3::FileListS3;
    use crate::pgpool::PgPool;
    use crate::s3_instance::S3Instance;

    #[tokio::test]
    #[ignore]
    async fn test_fill_file_list() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let s3 = S3Instance::new(&config.aws_region_name);
        let blist = s3.get_list_of_buckets().await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let mut flist = FileListS3::new(&bucket, &config, &pool)?.max_keys(100);

        let new_flist = flist.fill_file_list().await?;

        writeln!(stdout(), "{} {:?}", bucket, new_flist.get(0))?;
        assert!(new_flist.len() > 0);

        flist.with_list(new_flist);

        flist.cache_file_list()?;

        let new_flist = flist.load_file_list()?;

        assert_eq!(flist.flist.get_filemap().len(), new_flist.len());

        flist.clear_file_list()?;
        Ok(())
    }
}
