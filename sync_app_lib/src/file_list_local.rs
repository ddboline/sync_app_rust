use anyhow::{format_err, Error};
use async_trait::async_trait;
use futures::TryStreamExt;
use log::{debug, error};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use stack_string::StackString;
use std::{collections::HashMap, path::Path};
use stdout_channel::StdoutChannel;
use tokio::{
    fs::{copy, create_dir_all, remove_file, rename},
    task::{spawn, spawn_blocking, JoinHandle},
};
use url::Url;
use walkdir::WalkDir;

use crate::{
    config::Config,
    file_info::{FileInfoTrait, ServiceSession},
    file_info_local::FileInfoLocal,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    models::FileInfoCache,
    pgpool::PgPool,
};

#[derive(Debug, Clone)]
pub struct FileListLocal(pub FileList);

impl FileListLocal {
    /// # Errors
    /// Return error if init fails
    pub fn new(basedir: &Path, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let basepath = basedir.canonicalize()?;
        let basestr = basepath.to_string_lossy();
        let baseurl = Url::from_file_path(basepath.clone())
            .map_err(|e| format_err!("Failed to parse url {e:?}"))?;
        let session = basestr.parse()?;
        let flist = FileList::new(
            baseurl,
            basepath,
            config.clone(),
            FileService::Local,
            session,
            pool.clone(),
        );
        Ok(Self(flist))
    }

    /// # Errors
    /// Return error if init fails
    pub fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|e| format_err!("Parse failure {e:?}"))?;
            let basestr = path.to_string_lossy();
            let session = basestr.parse()?;
            let flist = FileList::new(
                url.clone(),
                path,
                config.clone(),
                FileService::Local,
                session,
                pool.clone(),
            );
            Ok(Self(flist))
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }
}

#[async_trait]
impl FileListTrait for FileListLocal {
    fn get_baseurl(&self) -> &Url {
        self.0.get_baseurl()
    }
    fn set_baseurl(&mut self, baseurl: Url) {
        self.0.set_baseurl(baseurl);
    }

    fn get_basepath(&self) -> &Path {
        &self.0.basepath
    }
    fn get_servicetype(&self) -> FileService {
        self.0.servicetype
    }
    fn get_servicesession(&self) -> &ServiceSession {
        &self.0.servicesession
    }
    fn get_config(&self) -> &Config {
        &self.0.config
    }
    fn get_pool(&self) -> &PgPool {
        &self.0.pool
    }

    async fn update_file_cache(&self) -> Result<usize, Error> {
        let servicesession = self.get_servicesession().clone();
        let basedir = self.get_baseurl().path();

        let wdir = WalkDir::new(basedir).same_file_system(true);
        let mut tasks = Vec::new();
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
        for entry in wdir {
            let entry = entry?;
            let filepath = entry.path().canonicalize().inspect_err(|e| {
                error!("error {e} entry {entry:?}",);
            })?;
            if filepath.is_dir() {
                continue;
            }
            let fileurl = Url::from_file_path(filepath.clone())
                .map_err(|e| format_err!("Failed to parse url {e:?}"))?;
            let metadata = entry.metadata()?;
            let size = metadata.len() as i32;
            if let Some(existing) = cached_urls.remove(fileurl.as_str()) {
                if existing.deleted_at.is_none() && existing.filestat_st_size == size {
                    continue;
                }
            }
            debug!("not in db {fileurl}");
            let pool = pool.clone();
            let servicesession = servicesession.clone();
            let task: JoinHandle<Result<usize, Error>> = spawn(async move {
                let info = spawn_blocking(move || {
                    FileInfoLocal::from_direntry(
                        &entry,
                        Some(servicesession.as_str().into()),
                        Some(servicesession),
                    )
                })
                .await??;

                let info: FileInfoCache = info.into_finfo().into();
                info.upsert(&pool).await
            });
            tasks.push(task);
        }
        for (_, missing) in cached_urls {
            if missing.deleted_at.is_some() || Path::new(&missing.filepath).exists() {
                continue;
            }
            missing.delete(pool).await?;
        }
        debug!("tasks {}", tasks.len());
        let mut number_updated = 0;
        for task in tasks {
            number_updated += task.await??;
        }
        Ok(number_updated)
    }

    async fn print_list(&self, stdout: &StdoutChannel<StackString>) -> Result<(), Error> {
        let local_list = self.clone();
        let stdout = stdout.clone();
        spawn_blocking(move || {
            let basedir = local_list.get_baseurl().path();

            let wdir = WalkDir::new(basedir).same_file_system(true).max_depth(1);

            let entries: Vec<_> = wdir.into_iter().filter_map(Result::ok).collect();

            entries
                .into_par_iter()
                .map(|entry| {
                    if let Ok(filepath) = entry.path().canonicalize() {
                        let filepath_str = filepath.to_string_lossy();
                        let filepath_str: StackString = filepath_str.as_ref().into();
                        stdout.send(filepath_str);
                    }
                    Ok(())
                })
                .collect()
        })
        .await?
    }

    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype != FileService::Local || finfo1.servicetype != FileService::Local {
            Err(format_err!(
                "Wrong fileinfo types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        } else {
            let local_file = &finfo1.filepath;
            let remote_file = &finfo0.filepath;
            let parent_dir = finfo1
                .filepath
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(&parent_dir).await?;
            }

            copy(&remote_file, &local_file).await?;
            Ok(())
        }
    }

    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        self.copy_from(finfo0, finfo1).await
    }

    async fn move_file(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        let path0 = &finfo0.filepath;
        let path1 = &finfo1.filepath;
        if finfo0.servicetype != FileService::Local || finfo1.servicetype != FileService::Local {
            Ok(())
        } else {
            if rename(&path0, path1).await.is_err() {
                copy(&path0, &path1).await?;
                remove_file(&path0).await?;
            }
            Ok(())
        }
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        if finfo.servicetype != FileService::Local {
            return Err(format_err!("Wrong service type"));
        } else if finfo.filepath.exists() {
            remove_file(&finfo.filepath).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use log::{debug, info};
    use stack_string::format_sstr;
    use std::{collections::HashMap, path::PathBuf};
    use url::Url;

    use crate::{
        config::Config,
        file_list_local::{FileListLocal, FileListTrait},
        file_service::FileService,
        pgpool::PgPool,
    };

    #[test]
    #[ignore]
    fn create_conf() -> Result<(), Error> {
        let basepath: PathBuf = "src".parse()?;
        let baseurl: Url =
            format_sstr!("file://{}", basepath.canonicalize()?.to_string_lossy()).parse()?;
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let conf = FileListLocal::new(&basepath, &config, &pool);
        debug!("{:?}", conf);
        assert_eq!(conf.is_ok(), true);
        let conf = conf?;
        assert_eq!(conf.get_servicetype(), FileService::Local);
        debug!("{:?}", conf.get_baseurl().as_str());
        assert_eq!(conf.get_baseurl(), &baseurl);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_fill_file_list() -> Result<(), Error> {
        let basepath: PathBuf = "src".parse()?;
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let flist = FileListLocal::new(&basepath, &config, &pool)?;

        flist.clear_file_list().await?;

        let updated = flist.update_file_cache().await?;

        info!("0 {updated}");

        let fset: HashMap<_, _> = flist
            .load_file_list(false)
            .await?
            .into_iter()
            .map(|f| (f.filename.clone(), f))
            .collect();

        assert_eq!(fset.contains_key("file_list_local.rs"), true);

        let cache_info = fset.get("file_list_local.rs").unwrap();

        info!("{:?}", cache_info);

        assert!(cache_info.filepath.ends_with("file_list_local.rs"));
        assert!(cache_info.urlname.as_str().ends_with("file_list_local.rs"));

        info!("{:?}", flist.get_servicesession());

        let new_flist = flist.load_file_list(false).await?;

        assert_eq!(new_flist.len(), updated);

        info!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        let updated = flist.update_file_cache().await?;

        assert_eq!(updated, 0);

        info!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        flist.clear_file_list().await?;

        let new_flist = flist.load_file_list(false).await?;

        assert_eq!(new_flist.len(), 0);
        Ok(())
    }
}
