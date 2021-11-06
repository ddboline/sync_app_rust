use anyhow::{format_err, Error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use log::debug;
use postgres_query::query;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
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
use tokio::task::spawn_blocking;
use url::Url;

use gdrive_lib::{directory_info::DirectoryInfo, gdrive_instance::GDriveInstance};
use stack_string::StackString;

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
    filemap: Arc<HashMap<StackString, FileInfo>>,
    inner: Arc<FileListInner>,
}

impl Deref for FileList {
    type Target = FileListInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl FileList {
    pub fn new(
        baseurl: Url,
        basepath: PathBuf,
        config: Config,
        servicetype: FileService,
        servicesession: ServiceSession,
        filemap: HashMap<StackString, FileInfo>,
        pool: PgPool,
    ) -> Self {
        Self {
            baseurl,
            filemap: Arc::new(filemap),
            inner: Arc::new(FileListInner {
                basepath,
                config,
                servicetype,
                servicesession,
                pool,
            }),
        }
    }

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
                let flist = FileListS3::from_url(url, config, pool)?;
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
    fn get_filemap(&self) -> &HashMap<StackString, FileInfo>;

    fn with_list(&mut self, filelist: Vec<FileInfo>);

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

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error>;

    async fn print_list(&self, _: &StdoutChannel<StackString>) -> Result<(), Error> {
        unimplemented!()
    }

    fn cleanup(&self) -> Result<(), Error> {
        if self.get_servicetype() == FileService::GDrive {
            let config = &self.get_config();
            let fname = config
                .gdrive_token_path
                .join(format!("{}_start_page_token", self.get_servicesession().0));
            let ext = fname
                .extension()
                .ok_or_else(|| format_err!("No extension"))?
                .to_string_lossy();
            let start_page_path = fname.with_extension(format!("{}.new", ext));
            debug!("{:?} {:?}", start_page_path, fname);
            if start_page_path.exists() {
                rename(&start_page_path, &fname).map_err(Into::into)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    async fn cache_file_list(&self) -> Result<usize, Error> {
        let pool = self.get_pool();

        // Load existing file_list, create hashmap
        let current_cache: HashMap<_, _> = self
            .load_file_list()
            .await?
            .into_iter()
            .filter_map(|item| {
                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        // Load and convert current filemap
        let flist_cache_map: HashMap<_, _> = self
            .get_filemap()
            .iter()
            .filter_map(|(_, f)| {
                let item: FileInfoCache = f.into();

                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        // Delete entries from current_cache not in filemap
        for k in current_cache.keys() {
            if flist_cache_map.contains_key(k) {
                continue;
            }
            println!("remove {:?}", k);
            k.delete_cache_entry(pool).await?;
        }

        for (k, v) in &flist_cache_map {
            if let Some(item) = current_cache.get(k) {
                if v.md5sum != item.md5sum
                    || v.sha1sum != item.sha1sum
                    || v.filestat_st_mtime != item.filestat_st_mtime
                    || v.filestat_st_size != item.filestat_st_size
                {
                    let mut cache = v
                        .get_cache(pool)
                        .await?
                        .ok_or_else(|| format_err!("Cache doesn't exist"))?;
                    if let Some(md5sum) = &v.md5sum {
                        cache.md5sum = Some(md5sum.clone());
                    }
                    if let Some(sha1sum) = &v.sha1sum {
                        cache.sha1sum = Some(sha1sum.clone());
                    }
                    cache.filestat_st_mtime = v.filestat_st_mtime;
                    cache.filestat_st_size = v.filestat_st_size;

                    println!("GOT HERE {:?}", cache);
                    cache.insert(pool).await?;
                }
            }
        }

        let mut inserted = 0;
        for (k, v) in flist_cache_map {
            if current_cache.contains_key(&k) {
                continue;
            }
            v.insert(pool).await?;
            inserted += 1;
        }
        Ok(inserted)
    }

    async fn load_file_list(&self) -> Result<Vec<FileInfoCache>, Error> {
        let session = self.get_servicesession();
        let stype = self.get_servicetype();
        let pool = self.get_pool();
        FileInfoCache::get_all_cached(&session.0, stype.to_str(), pool)
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

        DirectoryInfoCache::get_all(&session.0, stype.to_str(), pool)
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
            let is_root = root_id.as_ref().map_or(false, |rid| rid == &d.directory_id);

            let cache = DirectoryInfoCache {
                id: -1,
                directory_id: d.directory_id.as_str().into(),
                directory_name: d.directory_name.as_str().into(),
                parent_id: d.parentid.clone(),
                is_root,
                servicetype: self.get_servicetype().to_string().into(),
                servicesession: self.get_servicesession().0.as_str().into(),
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

        FileInfoCache::delete_all(&session.0, stype.to_str(), pool)
            .await
            .map_err(Into::into)
    }

    async fn remove_by_id(&self, gdriveid: &str) -> Result<usize, Error> {
        let pool = self.get_pool();
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        FileInfoCache::delete_by_id(gdriveid, &session.0, stype.to_str(), pool)
            .await
            .map_err(Into::into)
    }

    async fn clear_directory_list(&self) -> Result<usize, Error> {
        let pool = self.get_pool();
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        DirectoryInfoCache::delete_all(&session.0, stype.to_str(), pool)
            .await
            .map_err(Into::into)
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

    fn get_filemap(&self) -> &HashMap<StackString, FileInfo> {
        &self.filemap
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        let filemap = filelist
            .into_iter()
            .map(|f| {
                let path = f.filepath.to_string_lossy();
                let key = remove_basepath(&path, &self.get_basepath().to_string_lossy());
                let mut inner = f.inner().clone();
                inner.servicesession = self.get_servicesession().clone();
                let f = FileInfo::from_inner(inner);
                (key, f)
            })
            .collect();
        self.filemap = Arc::new(filemap);
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        self.load_file_list()
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect()
    }
}

pub fn remove_baseurl(urlname: &Url, baseurl: &Url) -> StackString {
    let baseurl = format!("{}/", baseurl.as_str().trim_end_matches('/'));
    urlname.as_str().replacen(&baseurl, "", 1).into()
}

pub fn replace_baseurl(urlname: &Url, baseurl0: &Url, baseurl1: &Url) -> Result<Url, Error> {
    let baseurl1 = baseurl1.as_str().trim_end_matches('/');

    let urlstr = format!("{}/{}", baseurl1, remove_baseurl(urlname, baseurl0));
    Url::parse(&urlstr).map_err(Into::into)
}

pub fn remove_basepath(basename: &str, basepath: &str) -> StackString {
    let basepath = format!("{}/", basepath.trim_end_matches('/'));
    basename.replacen(&basepath, "", 1).into()
}

pub fn replace_basepath(basename: &Path, basepath0: &Path, basepath1: &Path) -> PathBuf {
    let basepath0 = basepath0.to_string_lossy();
    let basepath1 = basepath1.to_string_lossy();
    let basepath1 = basepath1.trim_end_matches('/');

    let basename = basename.to_string_lossy();

    let new_path = format!("{}/{}", basepath1, remove_basepath(&basename, &basepath0));
    let new_path = Path::new(&new_path);
    new_path.to_path_buf()
}

pub fn group_urls(url_list: &[Url]) -> HashMap<StackString, Vec<Url>> {
    url_list.iter().fold(HashMap::new(), |mut h, m| {
        let key = m.scheme();
        h.entry(key.into()).or_insert_with(Vec::new).push(m.clone());
        h
    })
}
