use anyhow::{format_err, Error};
use async_trait::async_trait;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::HashMap,
    io::{stdout, Write},
    path::Path,
    string::ToString,
    time::SystemTime,
};
use tokio::{
    fs::{copy, create_dir_all, remove_file, rename},
    task::spawn_blocking,
};
use url::Url;
use walkdir::WalkDir;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession},
    file_info_local::FileInfoLocal,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    pgpool::PgPool,
};

#[derive(Debug, Clone)]
pub struct FileListLocal(pub FileList);

impl FileListLocal {
    pub fn new(basedir: &Path, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        let basepath = basedir.canonicalize()?;
        let basestr = basepath.to_string_lossy().to_string();
        let baseurl = Url::from_file_path(basepath.clone())
            .map_err(|_| format_err!("Failed to parse url"))?;
        let flist = FileList::new(
            baseurl,
            basepath,
            config.clone(),
            FileService::Local,
            basestr.parse()?,
            HashMap::new(),
            pool.clone(),
        );
        Ok(Self(flist))
    }

    pub fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|_| format_err!("Parse failure"))?;
            let basestr = path.to_string_lossy().to_string();
            let flist = FileList::new(
                url.clone(),
                path,
                config.clone(),
                FileService::Local,
                basestr.parse()?,
                HashMap::new(),
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

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        self.0.get_filemap()
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.0.with_list(filelist)
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let local_list = self.clone();
        spawn_blocking(move || {
            let servicesession = local_list.get_servicesession();
            let basedir = local_list.get_baseurl().path();
            let file_list = local_list.load_file_list()?;
            let flist_dict = local_list.get_file_list_dict(&file_list, FileInfoKeyType::FilePath);

            let wdir = WalkDir::new(&basedir).same_file_system(true);

            let entries: Vec<_> = wdir.into_iter().filter_map(Result::ok).collect();

            let flist = entries
                .into_par_iter()
                .filter_map(|entry| {
                    let filepath = entry
                        .path()
                        .canonicalize()
                        .ok()
                        .map_or_else(|| "".to_string(), |s| s.to_string_lossy().to_string());
                    let (modified, size) = entry.metadata().ok().map_or_else(
                        || (0, 0),
                        |metadata| {
                            let modified = metadata
                                .modified()
                                .unwrap()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as u32;
                            let size = metadata.len() as u32;
                            (modified, size)
                        },
                    );
                    if let Some(finfo) = flist_dict.get(&filepath) {
                        if let Some(fstat) = finfo.filestat {
                            if fstat.st_mtime >= modified && fstat.st_size == size {
                                return Some(finfo.clone());
                            }
                        }
                    };
                    FileInfoLocal::from_direntry(
                        &entry,
                        Some(servicesession.0.to_string().into()),
                        Some(servicesession.clone()),
                    )
                    .ok()
                    .map(|x| x.0)
                })
                .collect();

            Ok(flist)
        })
        .await?
    }

    async fn print_list(&self) -> Result<(), Error> {
        let local_list = self.clone();
        spawn_blocking(move || {
            let basedir = local_list.get_baseurl().path();

            let wdir = WalkDir::new(&basedir).same_file_system(true).max_depth(1);

            let entries: Vec<_> = wdir.into_iter().filter_map(Result::ok).collect();

            entries
                .into_par_iter()
                .map(|entry| {
                    let filepath = entry
                        .path()
                        .canonicalize()
                        .ok()
                        .map_or_else(|| "".to_string(), |s| s.to_string_lossy().to_string());
                    writeln!(stdout().lock(), "{}", filepath)?;
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
            let local_file = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?;
            let remote_file = finfo0
                .filepath
                .clone()
                .ok_or_else(|| format_err!("No local path"))?;
            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?
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
        let path0 = finfo0
            .filepath
            .as_ref()
            .ok_or_else(|| format_err!("No file path"))?;
        let path1 = finfo1
            .filepath
            .as_ref()
            .ok_or_else(|| format_err!("No file path"))?;
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
            Err(format_err!("Wrong service type"))
        } else if let Some(filepath) = finfo.filepath.as_ref() {
            remove_file(filepath).await.map_err(Into::into)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use log::debug;
    use std::{
        collections::HashMap,
        convert::TryInto,
        path::PathBuf,
    };
    use url::Url;

    use crate::{
        config::Config,
        file_info::FileInfo,
        file_list::FileList,
        file_list_local::{FileListLocal, FileListTrait},
        file_service::FileService,
        models::{FileInfoCache, InsertFileInfoCache},
        pgpool::PgPool,
    };

    #[test]
    #[ignore]
    fn create_conf() -> Result<(), Error> {
        let basepath: PathBuf = "src".parse()?;
        let baseurl: Url =
            format!("file://{}", basepath.canonicalize()?.to_string_lossy()).parse()?;
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let conf = FileListLocal::new(&basepath, &config, &pool);
        debug!("{:?}", conf);
        assert_eq!(conf.is_ok(), true);
        let conf = conf?;
        assert_eq!(conf.get_servicetype(), FileService::Local);
        debug!("{:?}", conf.get_baseurl());
        assert_eq!(conf.get_baseurl(), &baseurl);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_fill_file_list() -> Result<(), Error> {
        let basepath: PathBuf = "src".parse()?;
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let flist = FileListLocal::new(&basepath, &config, &pool)?;

        let new_flist = flist.fill_file_list().await?;

        debug!("0 {}", new_flist.len());

        let fset: HashMap<_, _> = new_flist
            .iter()
            .map(|f| (f.filename.clone(), f.clone()))
            .collect();

        assert_eq!(fset.contains_key("file_list_local.rs"), true);

        let result = fset.get("file_list_local.rs").unwrap();

        debug!("{:?}", result);

        assert!(result
            .filepath
            .as_ref()
            .unwrap()
            .ends_with("file_list_local.rs"));
        assert!(result
            .urlname
            .as_ref()
            .unwrap()
            .as_str()
            .ends_with("file_list_local.rs"));

        let cache_info: InsertFileInfoCache = result.into();
        debug!("{:?}", cache_info);
        assert_eq!(
            &result.md5sum.as_ref().unwrap().0,
            cache_info.md5sum.as_ref().unwrap()
        );

        let cache_info = &FileInfoCache::from_insert(cache_info, 5);
        let test_result: FileInfo = cache_info.try_into()?;
        assert_eq!(*result, test_result);

        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        debug!("1 {}", new_flist.len());

        let mut flist = FileListLocal::new(&basepath, &config, &pool)?;
        flist.with_list(new_flist);

        debug!("2 {}", flist.get_filemap().len());

        debug!("wrote {}", flist.cache_file_list()?);

        debug!("{:?}", flist.get_servicesession());

        let new_flist = flist.load_file_list()?;

        assert_eq!(new_flist.len(), flist.0.get_filemap().len());

        debug!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        let new_flist = flist.fill_file_list().await?;

        assert_eq!(new_flist.len(), flist.0.get_filemap().len());

        debug!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        flist.clear_file_list()?;

        let new_flist = flist.load_file_list()?;

        assert_eq!(new_flist.len(), 0);
        Ok(())
    }
}
