use anyhow::{format_err, Error};
use async_trait::async_trait;
use futures::TryStreamExt;
use log::info;
use stack_string::{format_sstr, StackString};
use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::Debug,
    fs::rename,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use stdout_channel::StdoutChannel;
use url::Url;
use uuid::Uuid;

use gdrive_lib::directory_info::DirectoryInfo;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession},
    file_list_gcs::FileListGcs,
    file_list_gdrive::FileListGDrive,
    file_list_local::FileListLocal,
    file_list_s3::FileListS3,
    file_list_ssh::FileListSSH,
    file_service::FileService,
    models::{DirectoryInfoCache, FileInfoCache},
    pgpool::PgPool,
};

#[derive(Clone, Debug)]
pub struct FileList {
    baseurl: Url,
    inner: Arc<FileListInner>,
}

impl Deref for FileList {
    type Target = FileListInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl FileList {
    #[must_use]
    pub fn new(
        baseurl: Url,
        basepath: PathBuf,
        config: Config,
        servicetype: FileService,
        servicesession: ServiceSession,
        pool: PgPool,
    ) -> Self {
        Self {
            baseurl,
            inner: Arc::new(FileListInner {
                basepath,
                config,
                servicetype,
                servicesession,
                pool,
            }),
        }
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn from_url(
        url: &Url,
        config: &Config,
        pool: &PgPool,
    ) -> Result<Box<dyn FileListTrait>, Error> {
        match url.scheme() {
            "gdrive" => {
                let flist = FileListGDrive::from_url(url, config, pool).await?;
                Ok(Box::new(flist))
            }
            "file" => {
                let flist = FileListLocal::from_url(url, config, pool)?;
                Ok(Box::new(flist))
            }
            "gs" => {
                let flist = FileListGcs::from_url(url, config, pool).await?;
                Ok(Box::new(flist))
            }
            "s3" => {
                let flist = FileListS3::from_url(url, config, pool).await?;
                Ok(Box::new(flist))
            }
            "ssh" => {
                let flist = FileListSSH::from_url(url, config, pool).await?;
                Ok(Box::new(flist))
            }
            _ => Err(format_err!("Bad scheme")),
        }
    }
}

#[derive(Debug)]
pub struct FileListInner {
    pub basepath: PathBuf,
    pub config: Config,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
    pub pool: PgPool,
}

#[async_trait]
pub trait FileListTrait: Send + Sync + Debug {
    fn get_baseurl(&self) -> &Url;
    fn set_baseurl(&mut self, baseurl: Url);
    fn get_basepath(&self) -> &Path;
    fn get_servicetype(&self) -> FileService;
    fn get_servicesession(&self) -> &ServiceSession;
    fn get_config(&self) -> &Config;

    fn get_pool(&self) -> &PgPool;

    // Copy operation where the origin (finfo0) has the same servicetype as self
    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        panic!("not implemented for {:?} {:?}", finfo0, finfo1);
    }

    // Copy operation where the destination (finfo0) has the same servicetype as
    // self
    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        panic!("not implemented for {:?} {:?}", finfo0, finfo1);
    }

    async fn move_file(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        panic!("not implemented for {:?} {:?}", finfo0, finfo1);
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        panic!("not implemented for {:?}", finfo);
    }

    /// Return updated `FileInfo` entries
    async fn update_file_cache(&self) -> Result<usize, Error>;

    async fn print_list(&self, _: &StdoutChannel<StackString>) -> Result<(), Error> {
        unimplemented!()
    }

    /// # Errors
    /// Return error if init fails
    fn cleanup(&self) -> Result<(), Error> {
        if self.get_servicetype() == FileService::GDrive {
            let config = &self.get_config();
            let token_str = format_sstr!("{}_start_page_token", self.get_servicesession().as_str());
            let fname = config.gdrive_token_path.join(token_str);
            let ext = fname
                .extension()
                .ok_or_else(|| format_err!("No extension"))?
                .to_string_lossy();
            let ext_str = format_sstr!("{ext}.new");
            let start_page_path = fname.with_extension(ext_str);
            info!("{} {}", start_page_path.display(), fname.display());
            if start_page_path.exists() {
                rename(&start_page_path, &fname).map_err(Into::into)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    async fn load_file_list(&self, get_deleted: bool) -> Result<Vec<FileInfoCache>, Error> {
        let session = self.get_servicesession();
        let stype = self.get_servicetype();
        let pool = self.get_pool();
        FileInfoCache::get_all_cached(session.as_str(), stype.to_str(), pool, get_deleted)
            .await?
            .try_collect()
            .await
            .map_err(Into::into)
    }

    fn get_file_list_dict(
        &self,
        file_list: &[FileInfoCache],
        key_type: FileInfoKeyType,
    ) -> HashMap<StackString, FileInfo> {
        file_list
            .iter()
            .filter_map(|entry| match key_type {
                FileInfoKeyType::FileName => entry
                    .try_into()
                    .ok()
                    .map(|val| (entry.filename.clone(), val)),
                FileInfoKeyType::FilePath => {
                    let key = entry.filepath.clone();
                    entry.try_into().ok().map(|val| (key, val))
                }
                FileInfoKeyType::UrlName => {
                    let key = entry.urlname.clone();
                    entry.try_into().ok().map(|val| (key, val))
                }
                FileInfoKeyType::Md5Sum => entry.md5sum.as_ref().and_then(|fp| {
                    let key = fp.clone();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::Sha1Sum => entry.sha1sum.as_ref().and_then(|fp| {
                    let key = fp.clone();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::ServiceId => {
                    let key = entry.serviceid.clone();
                    entry.try_into().ok().map(|val| (key, val))
                }
            })
            .collect()
    }

    async fn load_directory_info_cache(&self) -> Result<Vec<DirectoryInfoCache>, Error> {
        let session = self.get_servicesession();
        let stype = self.get_servicetype();
        let pool = self.get_pool();

        DirectoryInfoCache::get_all(session.as_str(), stype.to_str(), pool)
            .await?
            .try_collect()
            .await
            .map_err(Into::into)
    }

    fn get_directory_map_cache(
        &self,
        directory_list: Vec<DirectoryInfoCache>,
    ) -> (HashMap<StackString, DirectoryInfo>, Option<StackString>) {
        let root_id: Option<StackString> = directory_list.iter().find_map(|d| {
            if d.is_root {
                Some(d.directory_id.clone())
            } else {
                None
            }
        });
        let dmap: HashMap<_, _> = directory_list
            .into_iter()
            .map(|d| {
                let dinfo = d.into_directory_info();
                (dinfo.directory_id.clone(), dinfo)
            })
            .collect();
        (dmap, root_id)
    }

    async fn cache_directory_map(
        &self,
        directory_map: &HashMap<StackString, DirectoryInfo>,
        root_id: &Option<StackString>,
    ) -> Result<usize, Error> {
        let pool = self.get_pool();

        let mut inserted = 0;
        for d in directory_map.values() {
            let is_root = root_id.as_ref() == Some(&d.directory_id);
            let servicetype = StackString::from_display(self.get_servicetype());
            let cache = DirectoryInfoCache {
                id: Uuid::new_v4(),
                directory_id: d.directory_id.as_str().into(),
                directory_name: d.directory_name.as_str().into(),
                parent_id: d.parentid.clone(),
                is_root,
                servicetype,
                servicesession: self.get_servicesession().clone().into(),
            };

            cache.insert(pool).await?;
            inserted += 1;
        }
        Ok(inserted)
    }

    async fn clear_file_list(&self) -> Result<usize, Error> {
        let pool = self.get_pool();

        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        FileInfoCache::delete_all(session.as_str(), stype.to_str(), pool).await
    }

    async fn remove_by_id(&self, gdriveid: &str) -> Result<usize, Error> {
        let pool = self.get_pool();
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        FileInfoCache::delete_by_id(gdriveid, session.as_str(), stype.to_str(), pool).await
    }

    async fn clear_directory_list(&self) -> Result<usize, Error> {
        let pool = self.get_pool();
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        DirectoryInfoCache::delete_all(session.as_str(), stype.to_str(), pool).await
    }
}

#[async_trait]
impl FileListTrait for FileList {
    fn get_baseurl(&self) -> &Url {
        &self.baseurl
    }
    fn set_baseurl(&mut self, baseurl: Url) {
        self.baseurl = baseurl;
    }
    fn get_basepath(&self) -> &Path {
        &self.basepath
    }
    fn get_servicetype(&self) -> FileService {
        self.servicetype
    }
    fn get_servicesession(&self) -> &ServiceSession {
        &self.servicesession
    }
    fn get_config(&self) -> &Config {
        &self.config
    }

    fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    async fn update_file_cache(&self) -> Result<usize, Error> {
        Ok(0)
    }
}

#[must_use]
pub fn remove_baseurl(urlname: &Url, baseurl: &Url) -> StackString {
    let buf = format_sstr!("{}/", baseurl.as_str().trim_end_matches('/'));
    urlname.as_str().replacen(buf.as_str(), "", 1).into()
}

/// # Errors
/// Return error if init fails
pub fn replace_baseurl(urlname: &Url, baseurl0: &Url, baseurl1: &Url) -> Result<Url, Error> {
    let baseurl1 = baseurl1.as_str().trim_end_matches('/');
    let urlstr = format_sstr!("{}/{}", baseurl1, remove_baseurl(urlname, baseurl0));
    Url::parse(&urlstr).map_err(Into::into)
}

#[must_use]
pub fn remove_basepath(basename: &str, basepath: &str) -> StackString {
    let buf = format_sstr!("{}/", basepath.trim_end_matches('/'));
    basename.replacen(buf.as_str(), "", 1).into()
}

#[must_use]
pub fn replace_basepath(basename: &Path, basepath0: &Path, basepath1: &Path) -> PathBuf {
    let basepath0 = basepath0.to_string_lossy();
    let basepath1 = basepath1.to_string_lossy();
    let basepath1 = basepath1.trim_end_matches('/');

    let basename = basename.to_string_lossy();

    let new_path = Path::new(basepath1);
    new_path.join(remove_basepath(&basename, &basepath0))
}

#[must_use]
pub fn group_urls(url_list: &[Url]) -> HashMap<StackString, Vec<Url>> {
    url_list.iter().fold(HashMap::new(), |mut h, m| {
        let key = m.scheme();
        h.entry(key.into()).or_default().push(m.clone());
        h
    })
}
