use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use failure::{err_msg, Error};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::rename;
use std::path::{Path, PathBuf};
use url::Url;

use crate::config::Config;
use crate::directory_info::DirectoryInfo;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession};
use crate::file_list_gdrive::{FileListGDrive, FileListGDriveConf};
use crate::file_list_local::{FileListLocal, FileListLocalConf};
use crate::file_list_s3::{FileListS3, FileListS3Conf};
use crate::file_service::FileService;
use crate::gdrive_instance::GDriveInstance;
use crate::map_result;
use crate::models::{
    DirectoryInfoCache, FileInfoCache, InsertDirectoryInfoCache, InsertFileInfoCache,
};
use crate::pgpool::PgPool;

#[derive(Debug, Clone)]
pub struct FileListConf {
    pub baseurl: Url,
    pub basepath: PathBuf,
    pub config: Config,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
}

pub trait FileListConfTrait
where
    Self: Sized + Send + Sync,
{
    fn from_url(url: &Url, config: &Config) -> Result<Self, Error>;

    fn get_config(&self) -> &Config;
}

impl FileListConfTrait for FileListConf {
    fn from_url(url: &Url, config: &Config) -> Result<FileListConf, Error> {
        match url.scheme() {
            "gdrive" => FileListGDriveConf::from_url(url, config).map(|f| f.0),
            "file" => FileListLocalConf::from_url(url, config).map(|f| f.0),
            "s3" => FileListS3Conf::from_url(url, config).map(|f| f.0),
            _ => Err(err_msg("Bad scheme")),
        }
    }

    fn get_config(&self) -> &Config {
        &self.config
    }
}

#[derive(Debug, Clone)]
pub struct FileList {
    pub conf: FileListConf,
    pub filemap: HashMap<String, FileInfo>,
}

impl FileList {
    pub fn from_conf(conf: FileListConf) -> FileList {
        FileList {
            conf,
            filemap: HashMap::new(),
        }
    }

    pub fn with_list(&self, filelist: &[FileInfo]) -> FileList {
        FileList {
            conf: self.conf.clone(),
            filemap: filelist
                .iter()
                .map(|f| {
                    let key = if let Some(path) = f.filepath.as_ref().and_then(|x| x.to_str()) {
                        remove_basepath(&path, &self.conf.basepath.to_str().unwrap())
                    } else {
                        f.filename.clone()
                    };
                    let mut f = f.clone();
                    f.servicesession = Some(self.conf.servicesession.clone());
                    (key, f)
                })
                .collect(),
        }
    }
}

pub trait FileListTrait {
    fn get_conf(&self) -> &FileListConf;

    fn get_filemap(&self) -> &HashMap<String, FileInfo>;

    // Copy operation where the origin (finfo0) has the same servicetype as self
    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait;

    // Copy operation where the destination (finfo0) has the same servicetype as self
    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait;

    fn move_file<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait;

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait;

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error>;

    fn print_list(&self) -> Result<(), Error>;

    fn cleanup(&self) -> Result<(), Error> {
        if self.get_conf().servicetype == FileService::GDrive {
            let config = Config::init_config()?;
            let fname = format!(
                "{}/{}_start_page_token",
                config.gdrive_token_path,
                self.get_conf().servicesession.0
            );
            let start_page_path = format!("{}.new", fname);
            println!("{} {}", start_page_path, fname);
            rename(&start_page_path, &fname).map_err(err_msg)
        } else {
            Ok(())
        }
    }

    fn cache_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
        use crate::schema::file_info_cache;

        let current_cache: HashMap<_, _> = self
            .load_file_list(pool)?
            .into_iter()
            .filter_map(|item| {
                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        let flist_cache_map: HashMap<_, _> = self
            .get_filemap()
            .par_iter()
            .filter_map(|(_, f)| {
                let item: InsertFileInfoCache = f.into();

                let key = item.get_key();
                key.map(|k| (k, item))
            })
            .collect();

        let flist_cache_remove: Vec<_> = current_cache
            .par_iter()
            .filter_map(|(k, _)| match flist_cache_map.get(&k) {
                Some(_) => None,
                None => Some(k.clone()),
            })
            .collect();

        let results: Vec<_> = flist_cache_remove
            .into_par_iter()
            .map(|k| {
                use crate::schema::file_info_cache::dsl::{
                    file_info_cache, filename, filepath, serviceid, servicesession, urlname,
                };

                let conn = pool.get()?;

                println!("remove {:?}", k);

                diesel::delete(
                    file_info_cache
                        .filter(filename.eq(k.filename))
                        .filter(filepath.eq(k.filepath))
                        .filter(serviceid.eq(k.serviceid))
                        .filter(servicesession.eq(k.servicesession))
                        .filter(urlname.eq(k.urlname)),
                )
                .execute(&conn)
                .map_err(err_msg)
            })
            .collect();

        let _: Vec<_> = map_result(results)?;

        let flist_cache_update: Vec<_> = flist_cache_map
            .par_iter()
            .filter_map(|(k, v)| match current_cache.get(&k) {
                Some(item) => {
                    if v.md5sum != item.md5sum
                        || v.sha1sum != item.sha1sum
                        || v.filestat_st_mtime != item.filestat_st_mtime
                        || v.filestat_st_size != item.filestat_st_size
                    {
                        Some(v.clone())
                    } else {
                        None
                    }
                }
                None => None,
            })
            .collect();

        let results: Vec<_> = flist_cache_update
            .into_par_iter()
            .map(|v| {
                use crate::schema::file_info_cache::dsl::{
                    file_info_cache, filename, filepath, filestat_st_mtime, filestat_st_size, id,
                    md5sum, serviceid, servicesession, sha1sum, urlname,
                };

                let conn = pool.get()?;

                let cache = file_info_cache
                    .filter(filename.eq(v.filename))
                    .filter(filepath.eq(v.filepath))
                    .filter(urlname.eq(v.urlname))
                    .filter(serviceid.eq(v.serviceid))
                    .filter(servicesession.eq(v.servicesession))
                    .load::<FileInfoCache>(&conn)
                    .map_err(err_msg)?;
                if cache.len() != 1 {
                    return Err(err_msg("There should only be one entry"));
                }
                let id_ = cache[0].id;

                diesel::update(file_info_cache.filter(id.eq(id_)))
                    .set((
                        md5sum.eq(v.md5sum.clone()),
                        sha1sum.eq(v.sha1sum.clone()),
                        filestat_st_mtime.eq(v.filestat_st_mtime),
                        filestat_st_size.eq(v.filestat_st_size),
                    ))
                    .execute(&conn)
                    .map_err(err_msg)
            })
            .collect();

        let _: Vec<_> = map_result(results)?;

        let flist_cache_insert: Vec<_> = flist_cache_map
            .into_par_iter()
            .filter_map(|(k, v)| match current_cache.get(&k) {
                Some(_) => None,
                None => Some(v),
            })
            .collect();

        let results: Vec<_> = flist_cache_insert
            .chunks(1000)
            .map(|v| {
                diesel::insert_into(file_info_cache::table)
                    .values(v)
                    .execute(&pool.get()?)
                    .map_err(err_msg)
            })
            .collect();

        let results: Vec<_> = map_result(results)?;
        let result: usize = results.iter().sum();
        Ok(result)
    }

    fn load_file_list(&self, pool: &PgPool) -> Result<Vec<FileInfoCache>, Error> {
        use crate::schema::file_info_cache::dsl::{file_info_cache, servicesession, servicetype};

        let conn = pool.get()?;
        let conf = &self.get_conf();

        file_info_cache
            .filter(servicesession.eq(conf.servicesession.0.clone()))
            .filter(servicetype.eq(conf.servicetype.to_string()))
            .load::<FileInfoCache>(&conn)
            .map_err(err_msg)
    }

    fn get_file_list_dict(
        &self,
        file_list: Vec<FileInfoCache>,
        key_type: FileInfoKeyType,
    ) -> HashMap<String, FileInfo> {
        file_list
            .into_par_iter()
            .filter_map(|entry| match key_type {
                FileInfoKeyType::FileName => FileInfo::from_cache_info(&entry)
                    .ok()
                    .map(|val| (entry.filename.clone(), val)),
                FileInfoKeyType::FilePath => entry.filepath.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                }),
                FileInfoKeyType::UrlName => entry.urlname.as_ref().and_then(|url| {
                    let key = url.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                }),
                FileInfoKeyType::Md5Sum => entry.md5sum.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                }),
                FileInfoKeyType::Sha1Sum => entry.sha1sum.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                }),
                FileInfoKeyType::ServiceId => entry.serviceid.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                }),
            })
            .collect()
    }

    fn load_directory_info_cache(&self, pool: &PgPool) -> Result<Vec<DirectoryInfoCache>, Error> {
        use crate::schema::directory_info_cache::dsl::{
            directory_info_cache, servicesession, servicetype,
        };

        let conn = pool.get()?;
        let conf = &self.get_conf();

        directory_info_cache
            .filter(servicesession.eq(conf.servicesession.0.clone()))
            .filter(servicetype.eq(conf.servicetype.to_string()))
            .load::<DirectoryInfoCache>(&conn)
            .map_err(err_msg)
    }

    fn get_directory_map_cache(
        &self,
        directory_list: Vec<DirectoryInfoCache>,
    ) -> (HashMap<String, DirectoryInfo>, Option<String>) {
        let root_id: Option<String> = directory_list
            .iter()
            .filter(|d| d.is_root)
            .nth(0)
            .map(|d| d.directory_id.clone());
        let dmap: HashMap<_, _> = directory_list
            .into_par_iter()
            .map(|d| {
                let dinfo = DirectoryInfo::from_cache_info(&d);
                (d.directory_id, dinfo)
            })
            .collect();
        (dmap, root_id)
    }

    fn cache_directory_map(
        &self,
        pool: &PgPool,
        directory_map: &HashMap<String, DirectoryInfo>,
        root_id: &Option<String>,
    ) -> Result<usize, Error> {
        use crate::schema::directory_info_cache;

        let dmap_cache_insert: Vec<InsertDirectoryInfoCache> = directory_map
            .values()
            .map(|d| {
                let is_root = if let Some(rid) = root_id {
                    rid == &d.directory_id
                } else {
                    false
                };

                InsertDirectoryInfoCache {
                    directory_id: d.directory_id.clone(),
                    directory_name: d.directory_name.clone(),
                    parent_id: d.parentid.clone(),
                    is_root,
                    servicetype: self.get_conf().servicetype.to_string(),
                    servicesession: self.get_conf().servicesession.0.clone(),
                }
            })
            .collect();

        let results: Vec<_> = dmap_cache_insert
            .chunks(1000)
            .map(|v| {
                diesel::insert_into(directory_info_cache::table)
                    .values(v)
                    .execute(&pool.get()?)
                    .map_err(err_msg)
            })
            .collect();
        let results: Vec<_> = map_result(results)?;
        let result: usize = results.iter().sum();
        Ok(result)
    }

    fn clear_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
        use crate::schema::file_info_cache::dsl::{file_info_cache, servicesession, servicetype};

        let conn = pool.get()?;
        let conf = &self.get_conf();

        diesel::delete(
            file_info_cache
                .filter(servicesession.eq(conf.servicesession.0.clone()))
                .filter(servicetype.eq(conf.servicetype.to_string())),
        )
        .execute(&conn)
        .map_err(err_msg)
    }

    fn clear_directory_list(&self, pool: &PgPool) -> Result<usize, Error> {
        use crate::schema::directory_info_cache::dsl::{
            directory_info_cache, servicesession, servicetype,
        };

        let conn = pool.get()?;
        let conf = &self.get_conf();

        diesel::delete(
            directory_info_cache
                .filter(servicesession.eq(conf.servicesession.0.clone()))
                .filter(servicetype.eq(conf.servicetype.to_string())),
        )
        .execute(&conn)
        .map_err(err_msg)
    }
}

impl FileListTrait for FileList {
    fn get_conf(&self) -> &FileListConf {
        &self.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.filemap
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        match self.get_conf().servicetype {
            FileService::Local => {
                let conf = FileListLocalConf(self.get_conf().clone());
                let flist = FileListLocal::from_conf(conf);
                flist.fill_file_list(pool)
            }
            FileService::S3 => {
                let conf = FileListS3Conf(self.get_conf().clone());
                let flist = FileListS3::from_conf(conf);
                flist.fill_file_list(pool)
            }
            FileService::GDrive => {
                let conf = FileListGDriveConf(self.get_conf().clone());
                let config = Config::init_config()?;
                let gdrive = GDriveInstance::new(&config, &conf.0.servicesession.0);
                let flist = FileListGDrive::from_conf(conf, &gdrive)?;
                let flist = flist.set_directory_map(false, pool)?;
                flist.fill_file_list(pool)
            }
            _ => match pool {
                Some(pool) => match self.load_file_list(&pool) {
                    Ok(v) => {
                        let result: Vec<Result<_, Error>> =
                            v.iter().map(FileInfo::from_cache_info).collect();
                        map_result(result)
                    }
                    Err(e) => Err(e),
                },
                None => Ok(Vec::new()),
            },
        }
    }

    fn print_list(&self) -> Result<(), Error> {
        let conf = self.get_conf();
        match conf.servicetype {
            FileService::Local => {
                let fconf = FileListLocalConf(conf.clone());
                let flist = FileListLocal::from_conf(fconf);
                flist.print_list()
            }
            FileService::S3 => {
                let fconf = FileListS3Conf(conf.clone());
                let flist = FileListS3::from_conf(fconf);
                flist.print_list()
            }
            FileService::GDrive => {
                let config = Config::init_config()?;
                let fconf = FileListGDriveConf(conf.clone());
                let gdrive = GDriveInstance::new(&config, &fconf.0.servicesession.0);
                let flist = FileListGDrive::from_conf(fconf, &gdrive)?;
                let flist = flist.set_directory_map(false, None)?;
                flist.print_list()
            }
            _ => Err(err_msg("Not implemented")),
        }
    }

    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        match self.get_conf().servicetype {
            FileService::Local => {
                let f = FileListLocal(self.clone());
                f.copy_from(finfo0, finfo1)
            }
            FileService::S3 => {
                let c = FileListS3Conf(self.conf.clone());
                let f = FileListS3::from_conf(c);
                f.copy_from(finfo0, finfo1)
            }
            FileService::GDrive => {
                let c = FileListGDriveConf(self.conf.clone());
                let pool = PgPool::new(&c.get_config().database_url);
                let g = GDriveInstance::new(self.conf.get_config(), &self.conf.servicesession.0);
                let f = FileListGDrive::from_conf(c, &g)?.set_directory_map(true, Some(&pool))?;
                f.copy_from(finfo0, finfo1)
            }
            _ => Ok(()),
        }
    }

    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        match self.get_conf().servicetype {
            FileService::Local => {
                let f = FileListLocal(self.clone());
                f.copy_to(finfo0, finfo1)
            }
            FileService::S3 => {
                let c = FileListS3Conf(self.conf.clone());
                let f = FileListS3::from_conf(c);
                f.copy_to(finfo0, finfo1)
            }
            FileService::GDrive => {
                let c = FileListGDriveConf(self.conf.clone());
                let pool = PgPool::new(&c.get_config().database_url);
                let g = GDriveInstance::new(self.conf.get_config(), &self.conf.servicesession.0);
                let f = FileListGDrive::from_conf(c, &g)?.set_directory_map(true, Some(&pool))?;
                f.copy_to(finfo0, finfo1)
            }
            _ => Ok(()),
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
        match self.get_conf().servicetype {
            FileService::Local => {
                let f = FileListLocal(self.clone());
                f.move_file(finfo0, finfo1)
            }
            FileService::S3 => {
                let c = FileListS3Conf(self.conf.clone());
                let f = FileListS3::from_conf(c);
                f.move_file(finfo0, finfo1)
            }
            FileService::GDrive => {
                let c = FileListGDriveConf(self.conf.clone());
                let pool = PgPool::new(&c.get_config().database_url);
                let g = GDriveInstance::new(self.conf.get_config(), &self.conf.servicesession.0);
                let f = FileListGDrive::from_conf(c, &g)?.set_directory_map(true, Some(&pool))?;
                f.move_file(finfo0, finfo1)
            }
            _ => Ok(()),
        }
    }

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait,
    {
        match self.get_conf().servicetype {
            FileService::Local => {
                let conf = FileListLocalConf(self.get_conf().clone());
                let flist = FileListLocal::from_conf(conf);
                flist.delete(finfo)
            }
            FileService::S3 => {
                let conf = FileListS3Conf(self.get_conf().clone());
                let flist = FileListS3::from_conf(conf);
                flist.delete(finfo)
            }
            FileService::GDrive => {
                let c = FileListGDriveConf(self.get_conf().clone());
                let pool = PgPool::new(&c.get_config().database_url);
                let gdrive = GDriveInstance::new(&self.conf.get_config(), &c.0.servicesession.0);
                let flist =
                    FileListGDrive::from_conf(c, &gdrive)?.set_directory_map(true, Some(&pool))?;
                flist.delete(finfo)
            }
            _ => Ok(()),
        }
    }
}

pub fn remove_baseurl(urlname: &Url, baseurl: &Url) -> String {
    let baseurl = format!("{}/", baseurl.as_str().trim_end_matches('/'));
    urlname.as_str().replacen(&baseurl, "", 1)
}

pub fn replace_baseurl(urlname: &Url, baseurl0: &Url, baseurl1: &Url) -> Result<Url, Error> {
    let baseurl1 = baseurl1.as_str().trim_end_matches('/');

    let urlstr = format!("{}/{}", baseurl1, remove_baseurl(&urlname, baseurl0));
    Url::parse(&urlstr).map_err(err_msg)
}

pub fn remove_basepath(basename: &str, basepath: &str) -> String {
    let basepath = format!("{}/", basepath.trim_end_matches('/'));
    basename.replacen(&basepath, "", 1)
}

pub fn replace_basepath(
    basename: &Path,
    basepath0: &Path,
    basepath1: &Path,
) -> Result<PathBuf, Error> {
    let basepath0 = basepath0.to_str().ok_or_else(|| err_msg("Failure"))?;
    let basepath1 = basepath1
        .to_str()
        .ok_or_else(|| err_msg("Failure"))?
        .trim_end_matches('/');
    let basename = basename.to_str().ok_or_else(|| err_msg("Failure"))?;

    let new_path = format!("{}/{}", basepath1, remove_basepath(&basename, &basepath0));
    let new_path = Path::new(&new_path);
    Ok(new_path.to_path_buf())
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
