use anyhow::{format_err, Error};
use async_trait::async_trait;
use aws_types::region::Region;
use futures::TryStreamExt;
use log::{debug, info};
use stack_string::{format_sstr, StackString};
use std::{
    collections::HashMap,
    fs::{create_dir_all, remove_file},
    path::Path,
};
use stdout_channel::StdoutChannel;
use url::Url;

use crate::{
    config::Config,
    file_info::{FileInfoTrait, ServiceSession},
    file_info_s3::FileInfoS3,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    models::FileInfoCache,
    pgpool::PgPool,
    s3_instance::S3Instance,
};

#[derive(Debug, Clone)]
pub struct FileListS3 {
    pub flist: FileList,
    pub s3: S3Instance,
}

impl FileListS3 {
    /// # Errors
    /// Return error if init fails
    pub async fn new(bucket: &str, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let buf = format_sstr!("s3://{bucket}");
        let baseurl: Url = buf.parse()?;
        let basepath = Path::new("");

        let flist = FileList::new(
            baseurl,
            basepath.to_path_buf(),
            config.clone(),
            FileService::S3,
            bucket.parse()?,
            pool.clone(),
        );
        let region: String = config.aws_region_name.as_str().into();
        let region = Region::new(region);
        let sdk_config = aws_config::from_env().region(region).load().await;
        let s3 = S3Instance::new(&sdk_config);

        Ok(Self { flist, s3 })
    }

    /// # Errors
    /// Return error if init fails
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
                pool.clone(),
            );
            let region: String = config.aws_region_name.as_str().into();
            let region = Region::new(region);
            let sdk_config = aws_config::from_env().region(region).load().await;
            let s3 = S3Instance::new(&sdk_config);

            Ok(Self { flist, s3 })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }

    #[must_use]
    pub fn max_keys(mut self, max_keys: i32) -> Self {
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

    async fn update_file_cache(&self) -> Result<usize, Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');
        let mut number_updated = 0;
        let pool = self.get_pool();
        let mut cached_urls: HashMap<StackString, _> = FileInfoCache::get_all_cached(
            self.get_servicesession().as_str(),
            self.get_servicetype().to_str(),
            pool,
            false,
        )
        .await?
        .map_ok(|f| (f.urlname.clone(), f))
        .try_collect()
        .await?;
        debug!("expected {}", cached_urls.len());

        for object in self.s3.get_list_of_keys(bucket, Some(prefix)).await? {
            let info: FileInfoCache = FileInfoS3::from_object(bucket, object)?.into_finfo().into();
            if let Some(existing) = cached_urls.remove(&info.urlname) {
                if existing.deleted_at.is_none()
                    && existing.filestat_st_size == info.filestat_st_size
                {
                    continue;
                }
            }
            number_updated += info.upsert(pool).await?;
        }
        for (_, missing) in cached_urls {
            if missing.deleted_at.is_some() {
                continue;
            }
            missing.delete(pool).await?;
        }
        Ok(number_updated)
    }

    async fn print_list(&self, stdout: &StdoutChannel<StackString>) -> Result<(), Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');

        self.s3
            .process_list_of_keys(bucket, Some(prefix), |i| {
                let key = i.key.as_ref().map_or_else(|| "", String::as_str);
                let buf = format_sstr!("s3://{bucket}/{key}");
                stdout.send(buf);
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
            let local_file = finfo1.filepath.to_string_lossy();
            let parent_dir = finfo1
                .filepath
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(parent_dir)?;
            }
            let remote_url = finfo0.urlname.clone();
            let bucket = remote_url
                .host_str()
                .ok_or_else(|| format_err!("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            if Path::new(local_file.as_ref()).exists() {
                remove_file(local_file.as_ref())?;
            }
            let md5sum = self.s3.download(bucket, key, &local_file).await?;
            if md5sum != finfo1.md5sum.as_ref().map_or_else(|| "", |u| u.as_str()) {
                info!(
                    "Multipart upload? {} {}",
                    finfo1.urlname.as_str(),
                    finfo0.urlname.as_str(),
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
            let local_path = finfo0.filepath.canonicalize()?;
            let local_file = local_path.to_string_lossy();
            let remote_url = &finfo1.urlname;
            let bucket = remote_url
                .host_str()
                .ok_or_else(|| format_err!("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            self.s3.upload(&local_file, bucket, key).await
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
        let url0 = &finfo0.urlname;
        let bucket0 = url0.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key0 = url0.path();
        let url1 = &finfo1.urlname;
        let bucket1 = url1.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key1 = url1.path();
        let new_tag = self.s3.copy_key(url0, bucket1, key1).await?;
        if new_tag.is_some() {
            self.s3.delete_key(bucket0, key0).await?;
        }
        Ok(())
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        if finfo.servicetype == FileService::S3 {
            let url = &finfo.urlname;
            let bucket = url.host_str().ok_or_else(|| format_err!("No bucket"))?;
            let key = url.path();
            self.s3.delete_key(bucket, key).await
        } else {
            Err(format_err!("Wrong service type"))
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use aws_types::region::Region;
    use log::info;

    use crate::{
        config::Config, file_list::FileListTrait, file_list_s3::FileListS3, pgpool::PgPool,
        s3_instance::S3Instance,
    };

    #[tokio::test]
    #[ignore]
    #[allow(clippy::similar_names)]
    async fn test_fill_file_list() -> Result<(), Error> {
        let _guard = S3Instance::get_instance_lock();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let region: String = config.aws_region_name.as_str().into();
        let region = Region::new(region);
        let sdk_config = aws_config::from_env().region(region).load().await;
        let s3 = S3Instance::new(&sdk_config);
        let blist = s3.get_list_of_buckets().await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let flist = FileListS3::new(&bucket, &config, &pool)
            .await?
            .max_keys(100);
        flist.clear_file_list().await?;

        let updated = flist.update_file_cache().await?;

        info!("{bucket} {updated}");
        assert!(updated > 0);

        let new_flist = flist.load_file_list(false).await?;

        assert_eq!(updated, new_flist.len());

        flist.clear_file_list().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    #[allow(clippy::similar_names)]
    async fn test_list_buckets() -> Result<(), Error> {
        let _guard = S3Instance::get_instance_lock();
        let region = Region::from_static("us-east-1");
        let sdk_config = aws_config::from_env().region(region).load().await;
        let s3_instance = S3Instance::new(&sdk_config).max_keys(100);
        let blist = s3_instance.get_list_of_buckets().await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());
        let klist = s3_instance.get_list_of_keys(&bucket, None).await?;
        info!("{} {}", bucket, klist.len());
        assert!(klist.len() > 0);
        Ok(())
    }
}
