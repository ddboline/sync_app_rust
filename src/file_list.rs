use diesel::prelude::*;
use failure::{err_msg, Error};
use rayon::prelude::*;
use std::collections::HashMap;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait, ServiceSession};
use crate::file_list_gdrive::{FileListGDrive, FileListGDriveConf};
use crate::file_list_local::{FileListLocal, FileListLocalConf};
use crate::file_list_s3::{FileListS3, FileListS3Conf};
use crate::file_service::FileService;
use crate::gdrive_instance::GDriveInstance;
use crate::map_result_vec;
use crate::models::{FileInfoCache, InsertFileInfoCache};
use crate::pgpool::PgPool;
use crate::schema::file_info_cache;

#[derive(Debug, Clone)]
pub struct FileListConf {
    pub baseurl: Url,
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
                    let key = if let Some(url) = f.urlname.as_ref() {
                        remove_baseurl(&url, &self.conf.baseurl)
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

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();
        match conf.servicetype {
            FileService::Local => {
                let fconf = FileListLocalConf(conf.clone());
                let flist = FileListLocal::from_conf(fconf);
                flist.fill_file_list(pool)
            }
            FileService::S3 => {
                let fconf = FileListS3Conf(conf.clone());
                let flist = FileListS3::from_conf(fconf, None);
                flist.fill_file_list(pool)
            }
            FileService::GDrive => {
                let fconf = FileListGDriveConf(conf.clone());
                let config = Config::new();
                let gdrive = GDriveInstance::new(&config, &fconf.0.servicesession.0);
                let flist = FileListGDrive::from_conf(fconf, &gdrive)?;
                flist.fill_file_list(pool)
            }
            _ => Err(err_msg("Not implemented")),
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
                let flist = FileListS3::from_conf(fconf, None);
                flist.print_list()
            }
            FileService::GDrive => {
                let config = Config::new();
                let fconf = FileListGDriveConf(conf.clone());
                let gdrive = GDriveInstance::new(&config, &fconf.0.servicesession.0);
                let flist = FileListGDrive::from_conf(fconf, &gdrive)?;
                flist.print_list()
            }
            _ => Err(err_msg("Not implemented")),
        }
    }

    fn upload_file<T, U>(&self, finfo_local: &T, finfo_remote: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync;

    fn download_file<T, U>(&self, finfo_remote: &T, finfo_local: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync;

    fn cache_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
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
        let flist_cache_update: HashMap<_, _> = flist_cache_map
            .par_iter()
            .filter_map(|(k, v)| match current_cache.get(&k) {
                Some(item) => {
                    if v.md5sum != item.md5sum
                        || v.sha1sum != item.sha1sum
                        || v.filestat_st_mtime != item.filestat_st_mtime
                        || v.filestat_st_size != item.filestat_st_size
                    {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                }
                None => None,
            })
            .collect();

        let results = flist_cache_update
            .into_par_iter()
            .map(|(k, v)| {
                use crate::schema::file_info_cache::dsl::*;

                let conn = pool.get()?;

                let cache = file_info_cache
                    .filter(filename.eq(k.filename))
                    .filter(filepath.eq(k.filepath))
                    .filter(urlname.eq(k.urlname))
                    .filter(serviceid.eq(k.serviceid))
                    .filter(servicesession.eq(k.servicesession))
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

        map_result_vec(results)?;

        let flist_cache_insert: Vec<_> = flist_cache_map
            .into_par_iter()
            .filter_map(|(k, v)| match current_cache.get(&k) {
                Some(_) => None,
                None => Some(v),
            })
            .collect();

        let results = flist_cache_insert
            .chunks(1000)
            .map(|v| {
                diesel::insert_into(file_info_cache::table)
                    .values(v)
                    .execute(&pool.get()?)
                    .map_err(err_msg)
            })
            .collect();
        let result: usize = map_result_vec(results)?.iter().sum();
        Ok(result)
    }

    fn load_file_list(&self, pool: &PgPool) -> Result<Vec<FileInfoCache>, Error> {
        use crate::schema::file_info_cache::dsl::*;

        let conn = pool.get()?;
        let conf = &self.get_conf();

        file_info_cache
            .filter(servicesession.eq(conf.servicesession.0.clone()))
            .filter(servicetype.eq(conf.servicetype.to_string()))
            .load::<FileInfoCache>(&conn)
            .map_err(err_msg)
    }

    fn get_file_list_dict(&self, pool: &PgPool) -> Result<HashMap<String, FileInfo>, Error> {
        let flist_dict: HashMap<_, _> = self
            .load_file_list(&pool)?
            .into_par_iter()
            .filter_map(|entry| {
                entry.filepath.as_ref().and_then(|fp| {
                    let key = fp.to_string();
                    FileInfo::from_cache_info(&entry).ok().map(|val| (key, val))
                })
            })
            .collect();
        Ok(flist_dict)
    }

    fn clear_file_list(&self, pool: &PgPool) -> Result<usize, Error> {
        use crate::schema::file_info_cache::dsl::*;

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
                let flist = FileListS3::from_conf(conf, None);
                flist.fill_file_list(pool)
            }
            FileService::GDrive => {
                let conf = FileListGDriveConf(self.get_conf().clone());
                let config = Config::new();
                let gdrive = GDriveInstance::new(&config, &conf.0.servicesession.0);
                let flist = FileListGDrive::from_conf(conf, &gdrive)?;
                flist.fill_file_list(pool)
            }
            _ => match pool {
                Some(pool) => match self.load_file_list(&pool) {
                    Ok(v) => {
                        let result: Vec<Result<_, Error>> =
                            v.iter().map(FileInfo::from_cache_info).collect();
                        map_result_vec(result)
                    }
                    Err(e) => Err(e),
                },
                None => Ok(Vec::new()),
            },
        }
    }

    fn upload_file<T, U>(&self, finfo_local: &T, finfo_remote: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_local = finfo_local.get_finfo();
        let finfo_remote = finfo_remote.get_finfo();
        if finfo_local.servicetype != FileService::Local {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        if finfo_remote.servicetype == FileService::Local {
            let f = FileListLocal(self.clone());
            f.upload_file(finfo_local, finfo_remote)
        } else if finfo_remote.servicetype == FileService::S3 {
            let c = FileListS3Conf(self.conf.clone());
            let f = FileListS3::from_conf(c, None);
            f.upload_file(finfo_local, finfo_remote)
        } else {
            Ok(())
        }
    }

    fn download_file<T, U>(&self, finfo_remote: &T, finfo_local: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_local = finfo_local.get_finfo();
        let finfo_remote = finfo_remote.get_finfo();
        if finfo_local.servicetype != FileService::Local {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        if finfo_remote.servicetype == FileService::Local {
            let f = FileListLocal(self.clone());
            f.download_file(finfo_remote, finfo_local)
        } else if finfo_remote.servicetype == FileService::S3 {
            let c = FileListS3Conf(self.conf.clone());
            let f = FileListS3::from_conf(c, None);
            f.download_file(finfo_remote, finfo_local)
        } else {
            Ok(())
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

pub fn group_urls(url_list: &[Url]) -> HashMap<String, Vec<Url>> {
    url_list.into_iter().fold(HashMap::new(), |mut h, m| {
        let key = m.scheme();
        h.entry(key.to_string())
            .or_insert(Vec::new())
            .push(m.clone());
        h
    })
}
