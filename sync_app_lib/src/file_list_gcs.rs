use anyhow::{format_err, Error};
use async_trait::async_trait;
use checksums::{hash_file, Algorithm};
use log::info;
use std::{
    collections::HashMap,
    fmt::Write,
    fs::{create_dir_all, remove_file},
    path::Path,
};
use stdout_channel::StdoutChannel;
use tokio::task::spawn_blocking;
use url::Url;

use stack_string::StackString;

use gdrive_lib::gcs_instance::GcsInstance;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoTrait, ServiceSession},
    file_info_gcs::FileInfoGcs,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    pgpool::PgPool,
};

#[derive(Debug, Clone)]
pub struct FileListGcs {
    pub flist: FileList,
    pub gcs: GcsInstance,
}

impl FileListGcs {
    pub async fn new(bucket: &str, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let mut baseurl = StackString::new();
        write!(baseurl, "gs://{}", bucket)?;
        let baseurl: Url = baseurl.parse()?;
        let basepath = Path::new("");

        let flist = FileList::new(
            baseurl,
            basepath.to_path_buf(),
            config.clone(),
            FileService::GCS,
            bucket.parse()?,
            HashMap::new(),
            pool.clone(),
        );
        let gcs = GcsInstance::new(&config.gcs_token_path, &config.gcs_secret_file, bucket).await?;

        Ok(Self { flist, gcs })
    }

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
                HashMap::new(),
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

    fn get_filemap(&self) -> &HashMap<StackString, FileInfo> {
        self.flist.get_filemap()
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.flist.with_list(filelist);
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');

        self.gcs
            .get_list_of_keys(bucket, Some(prefix))
            .await?
            .into_iter()
            .map(|f| FileInfoGcs::from_object(bucket, f).map(FileInfoTrait::into_finfo))
            .collect()
    }

    async fn print_list(&self, stdout: &StdoutChannel<StackString>) -> Result<(), Error> {
        let bucket = self
            .get_baseurl()
            .host_str()
            .ok_or_else(|| format_err!("Parse error"))?;
        let prefix = self.get_baseurl().path().trim_start_matches('/');

        self.gcs
            .process_list_of_keys(bucket, Some(prefix), |i| {
                let mut buf = StackString::new();
                write!(
                    buf,
                    "gs://{}/{}",
                    bucket,
                    i.name.as_ref().map_or_else(|| "", String::as_str)
                )
                .unwrap();
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
                create_dir_all(&parent_dir)?;
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
            if md5sum != finfo1.md5sum.clone().map_or_else(|| "".into(), |u| u.0) {
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
        let pool = PgPool::new(&config.database_url);
        let gcs = GcsInstance::new(
            &config.gcs_token_path,
            &config.gcs_secret_file,
            "diary-backup-ddboline",
        )
        .await?;
        let blist = gcs.get_list_of_buckets(&config.gcs_project).await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let mut flist = FileListGcs::new(&bucket, &config, &pool).await?;

        let new_flist = flist.fill_file_list().await?;

        info!("{} {:?}", bucket, new_flist.get(0));
        assert!(new_flist.len() > 0);

        flist.with_list(new_flist);

        flist.cache_file_list().await?;

        let new_flist = flist.load_file_list().await?;

        assert_eq!(flist.flist.get_filemap().len(), new_flist.len());

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
            "diary-backup-ddboline",
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
