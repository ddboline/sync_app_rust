use anyhow::{format_err, Error};
use async_trait::async_trait;
use checksums::{hash_file, Algorithm};
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

use gdrive_lib::gcs_instance::GcsInstance;

use crate::{
    config::Config,
    file_info::{FileInfoTrait, ServiceSession},
    file_info_gcs::FileInfoGcs,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    models::FileInfoCache,
    pgpool::PgPool,
};

#[derive(Debug, Clone)]
pub struct FileListGcs {
    pub flist: FileList,
    pub gcs: GcsInstance,
}

impl FileListGcs {
    /// # Errors
    /// Return error if db query fails
    pub async fn new(bucket: &str, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let baseurl = format_sstr!("gs://{bucket}");
        let baseurl: Url = baseurl.parse()?;
        let basepath = Path::new("");

        let flist = FileList::new(
            baseurl,
            basepath.to_path_buf(),
            config.clone(),
            FileService::GCS,
            bucket.parse()?,
            pool.clone(),
        );
        let gcs = GcsInstance::new(&config.gcs_token_path, &config.gcs_secret_file, bucket).await?;

        Ok(Self { flist, gcs })
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "gs" {
            let basepath = Path::new(url.path());
            let bucket = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
            let flist = FileList::new(
                url.clone(),
                basepath.to_path_buf(),
                config.clone(),
                FileService::GCS,
                bucket.parse()?,
                pool.clone(),
            );
            let config = config.clone();
            let gcs =
                GcsInstance::new(&config.gcs_token_path, &config.gcs_secret_file, bucket).await?;

            Ok(Self { flist, gcs })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }
}

#[async_trait]
impl FileListTrait for FileListGcs {
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

        for object in self.gcs.get_list_of_keys(bucket, Some(prefix)).await? {
            let info: FileInfoCache = FileInfoGcs::from_object(bucket, object)?
                .into_finfo()
                .into();
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

        self.gcs
            .process_list_of_keys(bucket, Some(prefix), |i| {
                let key = i.name.as_ref().map_or_else(|| "", String::as_str);
                let buf = format_sstr!("gs://{bucket}/{key}");
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
        if finfo0.servicetype == FileService::GCS && finfo1.servicetype == FileService::Local {
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
            self.gcs.download(bucket, key, &local_file).await?;
            let md5sum: StackString = hash_file(Path::new(local_file.as_ref()), Algorithm::MD5)
                .to_lowercase()
                .into();
            if md5sum != finfo1.md5sum.as_ref().map_or_else(|| "", |m| m.as_str()) {
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
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::GCS {
            let local_path = finfo0.filepath.canonicalize()?;
            let local_file = local_path.to_string_lossy();
            let remote_url = &finfo1.urlname;
            let bucket = remote_url
                .host_str()
                .ok_or_else(|| format_err!("No bucket"))?;
            let key = remote_url.path().trim_start_matches('/');
            self.gcs.upload(&local_file, bucket, key).await
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
        let key0 = url0.path().trim_start_matches('/');
        let url1 = &finfo1.urlname;
        let bucket1 = url1.host_str().ok_or_else(|| format_err!("Parse error"))?;
        let key1 = url1.path().trim_start_matches('/');
        let new_tag = self.gcs.copy_key(url0, bucket1, key1).await?;
        if new_tag.is_some() {
            self.gcs.delete_key(bucket0, key0).await?;
        }
        Ok(())
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        if finfo.servicetype == FileService::GCS {
            let url = &finfo.urlname;
            let bucket = url.host_str().ok_or_else(|| format_err!("No bucket"))?;
            let key = url.path().trim_start_matches('/');
            self.gcs.delete_key(bucket, key).await
        } else {
            Err(format_err!("Wrong service type"))
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use log::info;

    use gdrive_lib::gcs_instance::GcsInstance;

    use crate::{
        config::Config, file_list::FileListTrait, file_list_gcs::FileListGcs, pgpool::PgPool,
    };

    #[tokio::test]
    #[ignore]
    #[allow(clippy::similar_names)]
    async fn test_fill_file_list() -> Result<(), Error> {
        let _guard = GcsInstance::get_instance_lock();
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let gcs = GcsInstance::new(
            &config.gcs_token_path,
            &config.gcs_secret_file,
            "diary-backup-ddboline-2024-06-30",
        )
        .await?;
        let blist = gcs.get_list_of_buckets(&config.gcs_project).await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let flist = FileListGcs::new(&bucket, &config, &pool).await?;

        flist.clear_file_list().await?;

        let number_updated = flist.update_file_cache().await?;

        info!("{} {}", bucket, number_updated);
        assert!(number_updated > 0);

        let new_flist = flist.load_file_list(false).await?;

        assert_eq!(number_updated, new_flist.len());

        flist.clear_file_list().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    #[allow(clippy::similar_names)]
    async fn test_list_buckets() -> Result<(), Error> {
        let _guard = GcsInstance::get_instance_lock();
        let config = Config::init_config()?;
        info!("{:?} {:?}", config.gcs_token_path, config.gcs_secret_file);
        let gcs_instance = GcsInstance::new(
            &config.gcs_token_path,
            &config.gcs_secret_file,
            "diary-backup-ddboline-2024-06-30",
        )
        .await?;
        let blist = gcs_instance
            .get_list_of_buckets(&config.gcs_project)
            .await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());
        let klist = gcs_instance.get_list_of_keys(&bucket, None).await?;
        info!("{} {}", bucket, klist.len());
        assert!(klist.len() > 0);
        Ok(())
    }
}
