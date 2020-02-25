use anyhow::{format_err, Error};
use async_trait::async_trait;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use log::debug;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::fs::rename;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::spawn_blocking;
use url::Url;

use gdrive_lib::directory_info::DirectoryInfo;
use gdrive_lib::gdrive_instance::GDriveInstance;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession};
use crate::file_list_gdrive::FileListGDrive;
use crate::file_list_local::FileListLocal;
use crate::file_list_s3::FileListS3;
use crate::file_list_ssh::FileListSSH;
use crate::file_service::FileService;
use crate::models::{
    DirectoryInfoCache, FileInfoCache, InsertDirectoryInfoCache, InsertFileInfoCache,
};
use crate::pgpool::PgPool;

#[derive(Clone, Debug)]
pub struct FileList {
    baseurl: Url,
    filemap: Arc<HashMap<String, FileInfo>>,
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
        filemap: HashMap<String, FileInfo>,
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
            "s3" => {
                let flist = FileListS3::from_url(url, config, pool)?;
                Ok(Box::new(flist))
            }
            "ssh" => {
                let flist = FileListSSH::from_url(url, config, pool)?;
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
    fn get_filemap(&self) -> &HashMap<String, FileInfo>;

    fn with_list(&mut self, filelist: Vec<FileInfo>);

    // Copy operation where the origin (finfo0) has the same servicetype as self
    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        panic!("not implemented for {:?} {:?}", finfo0, finfo1);
    }

    // Copy operation where the destination (finfo0) has the same servicetype as self
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

    async fn print_list(&self) -> Result<(), Error> {
        unimplemented!()
    }

    fn cleanup(&self) -> Result<(), Error> {
        if self.get_servicetype() == FileService::GDrive {
            let config = &self.get_config();
            let fname = format!(
                "{}/{}_start_page_token",
                config.gdrive_token_path,
                self.get_servicesession().0
            );
            let start_page_path = format!("{}.new", fname);
            debug!("{} {}", start_page_path, fname);
            if Path::new(&start_page_path).exists() {
                rename(&start_page_path, &fname).map_err(Into::into)
            } else {
                Ok(())
            }
        } else {
            Ok(())
        }
    }

    fn cache_file_list(&self) -> Result<usize, Error> {
        use crate::schema::file_info_cache;
        let pool = self.get_pool();

        // Load existing file_list, create hashmap
        let current_cache: HashMap<_, _> = self
            .load_file_list()?
            .into_par_iter()
            .filter_map(|item| {
                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        // Load and convert current filemap
        let flist_cache_map: HashMap<_, _> = self
            .get_filemap()
            .par_iter()
            .filter_map(|(_, f)| {
                let item: InsertFileInfoCache = f.into();

                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        // Delete entries from current_cache not in filemap
        let results: Result<(), Error> = current_cache
            .par_iter()
            .map(|(k, _)| match flist_cache_map.get(&k) {
                Some(_) => Ok(()),
                None => {
                    use crate::schema::file_info_cache::dsl::{
                        file_info_cache, filename, filepath, serviceid, servicesession, urlname,
                    };

                    let conn = pool.get()?;

                    debug!("remove {:?}", k);

                    diesel::delete(
                        file_info_cache
                            .filter(filename.eq(&k.filename))
                            .filter(filepath.eq(&k.filepath))
                            .filter(serviceid.eq(&k.serviceid))
                            .filter(servicesession.eq(&k.servicesession))
                            .filter(urlname.eq(&k.urlname)),
                    )
                    .execute(&conn)?;
                    Ok(())
                }
            })
            .collect();
        results?;

        let results: Result<(), Error> = flist_cache_map
            .par_iter()
            .map(|(k, v)| {
                if let Some(item) = current_cache.get(&k) {
                    if v.md5sum != item.md5sum
                        || v.sha1sum != item.sha1sum
                        || v.filestat_st_mtime != item.filestat_st_mtime
                        || v.filestat_st_size != item.filestat_st_size
                    {
                        use crate::schema::file_info_cache::dsl::{
                            file_info_cache, filename, filepath, filestat_st_mtime,
                            filestat_st_size, id, md5sum, serviceid, servicesession, sha1sum,
                            urlname,
                        };

                        let conn = pool.get()?;

                        let cache = file_info_cache
                            .filter(filename.eq(&v.filename))
                            .filter(filepath.eq(&v.filepath))
                            .filter(urlname.eq(&v.urlname))
                            .filter(serviceid.eq(&v.serviceid))
                            .filter(servicesession.eq(&v.servicesession))
                            .load::<FileInfoCache>(&conn)?;
                        if cache.len() != 1 {
                            return Err(format_err!("There should only be one entry"));
                        }
                        let id_ = cache[0].id;

                        diesel::update(file_info_cache.filter(id.eq(id_)))
                            .set((
                                md5sum.eq(&v.md5sum),
                                sha1sum.eq(&v.sha1sum),
                                filestat_st_mtime.eq(v.filestat_st_mtime),
                                filestat_st_size.eq(v.filestat_st_size),
                            ))
                            .execute(&conn)?;
                    }
                }
                Ok(())
            })
            .collect();
        results?;

        let flist_cache_insert: Vec<_> = flist_cache_map
            .into_par_iter()
            .filter_map(|(k, v)| {
                if current_cache.get(&k).is_none() {
                    Some(v)
                } else {
                    None
                }
            })
            .collect();

        let results: Result<Vec<_>, Error> = flist_cache_insert
            .into_par_iter()
            .chunks(1000)
            .map(|v| {
                diesel::insert_into(file_info_cache::table)
                    .values(v)
                    .execute(&pool.get()?)
                    .map_err(Into::into)
            })
            .collect();

        let result: usize = results?.iter().sum();
        Ok(result)
    }

    fn load_file_list(&self) -> Result<Vec<FileInfoCache>, Error> {
        use crate::schema::file_info_cache::dsl::{file_info_cache, servicesession, servicetype};

        let pool = self.get_pool();
        let conn = pool.get()?;
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        file_info_cache
            .filter(servicesession.eq(session.0.to_string()))
            .filter(servicetype.eq(stype.to_string()))
            .load::<FileInfoCache>(&conn)
            .map_err(Into::into)
    }

    fn get_file_list_dict(
        &self,
        file_list: &[FileInfoCache],
        key_type: FileInfoKeyType,
    ) -> HashMap<String, FileInfo> {
        file_list
            .par_iter()
            .filter_map(|entry| match key_type {
                FileInfoKeyType::FileName => entry
                    .try_into()
                    .ok()
                    .map(|val| (entry.filename.to_string(), val)),
                FileInfoKeyType::FilePath => entry.filepath.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::UrlName => entry.urlname.as_ref().and_then(|url| {
                    let key = url.to_string();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::Md5Sum => entry.md5sum.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::Sha1Sum => entry.sha1sum.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    entry.try_into().ok().map(|val| (key, val))
                }),
                FileInfoKeyType::ServiceId => entry.serviceid.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    entry.try_into().ok().map(|val| (key, val))
                }),
            })
            .collect()
    }

    fn load_directory_info_cache(&self) -> Result<Vec<DirectoryInfoCache>, Error> {
        use crate::schema::directory_info_cache::dsl::{
            directory_info_cache, servicesession, servicetype,
        };

        let pool = self.get_pool();
        let conn = pool.get()?;
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        directory_info_cache
            .filter(servicesession.eq(session.0.to_string()))
            .filter(servicetype.eq(stype.to_string()))
            .load::<DirectoryInfoCache>(&conn)
            .map_err(Into::into)
    }

    fn get_directory_map_cache(
        &self,
        directory_list: Vec<DirectoryInfoCache>,
    ) -> (HashMap<String, DirectoryInfo>, Option<String>) {
        let root_id: Option<String> = directory_list
            .iter()
            .filter(|d| d.is_root)
            .nth(0)
            .map(|d| d.directory_id.to_string());
        let dmap: HashMap<_, _> = directory_list
            .into_par_iter()
            .map(|d| {
                let dinfo = d.into_directory_info();
                (dinfo.directory_id.clone(), dinfo)
            })
            .collect();
        (dmap, root_id)
    }

    fn cache_directory_map(
        &self,
        directory_map: &HashMap<String, DirectoryInfo>,
        root_id: &Option<String>,
    ) -> Result<usize, Error> {
        use crate::schema::directory_info_cache;

        let pool = self.get_pool();

        let dmap_cache_insert: Vec<InsertDirectoryInfoCache> = directory_map
            .values()
            .map(|d| {
                let is_root = if let Some(rid) = root_id {
                    rid == &d.directory_id
                } else {
                    false
                };

                InsertDirectoryInfoCache {
                    directory_id: d.directory_id.to_string(),
                    directory_name: d.directory_name.to_string(),
                    parent_id: d.parentid.clone(),
                    is_root,
                    servicetype: self.get_servicetype().to_string(),
                    servicesession: self.get_servicesession().0.to_string(),
                }
            })
            .collect();

        let results: Result<Vec<_>, Error> = dmap_cache_insert
            .chunks(1000)
            .map(|v| {
                diesel::insert_into(directory_info_cache::table)
                    .values(v)
                    .execute(&pool.get()?)
                    .map_err(Into::into)
            })
            .collect();
        let result: usize = results?.iter().sum();
        Ok(result)
    }

    fn clear_file_list(&self) -> Result<usize, Error> {
        use crate::schema::file_info_cache::dsl::{file_info_cache, servicesession, servicetype};

        let pool = self.get_pool();

        let conn = pool.get()?;
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        diesel::delete(
            file_info_cache
                .filter(servicesession.eq(session.0.to_string()))
                .filter(servicetype.eq(stype.to_string())),
        )
        .execute(&conn)
        .map_err(Into::into)
    }

    fn remove_by_id(&self, gdriveid: &str) -> Result<usize, Error> {
        use crate::schema::file_info_cache::dsl::{
            file_info_cache, serviceid, servicesession, servicetype,
        };

        let pool = self.get_pool();
        let conn = pool.get()?;
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        diesel::delete(
            file_info_cache
                .filter(servicesession.eq(session.0.to_string()))
                .filter(servicetype.eq(stype.to_string()))
                .filter(serviceid.eq(Some(gdriveid))),
        )
        .execute(&conn)
        .map_err(Into::into)
    }

    fn clear_directory_list(&self) -> Result<usize, Error> {
        use crate::schema::directory_info_cache::dsl::{
            directory_info_cache, servicesession, servicetype,
        };

        let pool = self.get_pool();
        let conn = pool.get()?;
        let session = self.get_servicesession();
        let stype = self.get_servicetype();

        diesel::delete(
            directory_info_cache
                .filter(servicesession.eq(session.0.to_string()))
                .filter(servicetype.eq(stype.to_string())),
        )
        .execute(&conn)
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

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.filemap
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        let filemap = filelist
            .into_par_iter()
            .map(|f| {
                let key = if let Some(path) = f.filepath.as_ref().map(|x| x.to_string_lossy()) {
                    remove_basepath(&path, &self.get_basepath().to_string_lossy())
                } else {
                    f.filename.to_string()
                };
                let mut inner = f.inner().clone();
                inner.servicesession = Some(self.get_servicesession().clone());
                let f = FileInfo::from_inner(inner);
                (key, f)
            })
            .collect();
        self.filemap = Arc::new(filemap);
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let self_ = self.clone();
        spawn_blocking(move || match self_.load_file_list() {
            Ok(v) => {
                let result: Result<Vec<_>, Error> = v.par_iter().map(TryInto::try_into).collect();
                result
            }
            Err(e) => Err(e),
        })
        .await?
    }
}

pub fn remove_baseurl(urlname: &Url, baseurl: &Url) -> String {
    let baseurl = format!("{}/", baseurl.as_str().trim_end_matches('/'));
    urlname.as_str().replacen(&baseurl, "", 1)
}

pub fn replace_baseurl(urlname: &Url, baseurl0: &Url, baseurl1: &Url) -> Result<Url, Error> {
    let baseurl1 = baseurl1.as_str().trim_end_matches('/');

    let urlstr = format!("{}/{}", baseurl1, remove_baseurl(&urlname, baseurl0));
    Url::parse(&urlstr).map_err(Into::into)
}

pub fn remove_basepath(basename: &str, basepath: &str) -> String {
    let basepath = format!("{}/", basepath.trim_end_matches('/'));
    basename.replacen(&basepath, "", 1)
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

pub fn group_urls(url_list: &[Url]) -> HashMap<String, Vec<Url>> {
    url_list.iter().fold(HashMap::new(), |mut h, m| {
        let key = m.scheme();
        h.entry(key.to_string())
            .or_insert_with(Vec::new)
            .push(m.clone());
        h
    })
}
