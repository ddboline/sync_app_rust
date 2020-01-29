use anyhow::{format_err, Error};
use fmt::Debug;
use log::debug;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::convert::From;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait};
use crate::file_list::{
    group_urls, replace_basepath, replace_baseurl, FileList, FileListConf, FileListConfTrait,
    FileListTrait,
};
use crate::file_service::FileService;
use crate::models::{FileSyncCache, InsertFileSyncCache};
use crate::pgpool::PgPool;

#[derive(Debug)]
pub enum FileSyncAction {
    Index,
    Sync,
    Process,
    Copy,
    List,
    Delete,
    Move,
    Serialize,
    AddConfig,
    ShowCache,
}

impl FromStr for FileSyncAction {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "index" => Ok(Self::Index),
            "sync" => Ok(Self::Sync),
            "process" | "proc" => Ok(Self::Process),
            "copy" | "cp" => Ok(Self::Copy),
            "list" | "ls" => Ok(Self::List),
            "delete" | "rm" => Ok(Self::Delete),
            "move" | "mv" => Ok(Self::Move),
            "ser" | "serialize" => Ok(Self::Serialize),
            "add" | "add_config" => Ok(Self::AddConfig),
            "show" | "show_cache" => Ok(Self::ShowCache),
            _ => Err(format_err!("Parse failure")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FileSyncMode {
    OutputFile(PathBuf),
    Full,
}

impl Default for FileSyncMode {
    fn default() -> Self {
        Self::Full
    }
}

impl fmt::Display for FileSyncMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Full => write!(f, "Full"),
            Self::OutputFile(pathbuf) => write!(f, "OutputFile({})", pathbuf.to_string_lossy()),
        }
    }
}

impl From<&str> for FileSyncMode {
    fn from(s: &str) -> Self {
        if s == "full" {
            Self::Full
        } else {
            Self::OutputFile(Path::new(&s).to_path_buf())
        }
    }
}

#[derive(Default, Debug)]
pub struct FileSync {
    pub config: Config,
}

impl FileSync {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn compare_lists<T, U>(flist0: &T, flist1: &U, pool: &PgPool) -> Result<(), Error>
    where
        T: FileListTrait + Send + Sync + Debug,
        U: FileListTrait + Send + Sync + Debug,
    {
        let conf0 = flist0.get_conf();
        let conf1 = flist1.get_conf();
        let list_a_not_b: Vec<_> = flist0
            .get_filemap()
            .par_iter()
            .filter_map(|(k, finfo0)| match flist1.get_filemap().get(k) {
                Some(finfo1) => {
                    if Self::compare_objects(finfo0, finfo1) {
                        Some((finfo0.clone(), finfo1.clone()))
                    } else {
                        None
                    }
                }
                None => {
                    if let Some(path0) = finfo0.filepath.as_ref() {
                        let url0 = finfo0.urlname.as_ref().unwrap();
                        if let Ok(url1) = replace_baseurl(&url0, &conf0.baseurl, &conf1.baseurl) {
                            let path1 = replace_basepath(&path0, &conf0.basepath, &conf1.basepath);
                            if url1.as_str().contains(conf1.baseurl.as_str()) {
                                let finfo1 = FileInfo {
                                    filename: k.clone(),
                                    filepath: Some(path1),
                                    urlname: Some(url1),
                                    servicesession: Some(conf1.servicesession.clone()),
                                    servicetype: conf1.servicetype,
                                    serviceid: Some(conf1.servicesession.clone().into()),
                                    ..FileInfo::default()
                                };
                                debug!("ab {:?} {:?}", finfo0, finfo1);
                                Some((finfo0.clone(), finfo1))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            })
            .collect();
        let list_b_not_a: Vec<_> = flist1
            .get_filemap()
            .par_iter()
            .filter_map(|(k, finfo1)| match flist0.get_filemap().get(k) {
                Some(_) => None,
                None => {
                    if let Some(path1) = finfo1.filepath.as_ref() {
                        let url1 = finfo1.urlname.as_ref().unwrap();
                        if let Ok(url0) = replace_baseurl(&url1, &conf1.baseurl, &conf0.baseurl) {
                            let path0 = replace_basepath(&path1, &conf1.basepath, &conf0.basepath);
                            if url0.as_str().contains(conf0.baseurl.as_str()) {
                                let finfo0 = FileInfo {
                                    filename: k.clone(),
                                    filepath: Some(path0),
                                    urlname: Some(url0),
                                    servicesession: Some(conf0.servicesession.clone()),
                                    servicetype: conf0.servicetype,
                                    serviceid: Some(conf0.servicesession.clone().into()),
                                    ..FileInfo::default()
                                };
                                debug!("ba {:?} {:?}", finfo0, finfo1);
                                Some((finfo1.clone(), finfo0))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            })
            .collect();
        debug!("ab {} ba {}", list_a_not_b.len(), list_b_not_a.len());
        if list_a_not_b.is_empty() && list_b_not_a.is_empty() {
            flist0.cleanup().and_then(|_| flist1.cleanup())
        } else {
            list_a_not_b
                .par_iter()
                .chain(list_b_not_a.par_iter())
                .map(|(f0, f1)| {
                    if let Some(u0) = f0.urlname.as_ref() {
                        if let Some(u1) = f1.urlname.as_ref() {
                            InsertFileSyncCache::cache_sync(pool, u0.as_str(), u1.as_str())?;
                        }
                    }
                    Ok(())
                })
                .collect()
        }
    }

    pub fn compare_objects<T, U>(finfo0: &T, finfo1: &U) -> bool
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();

        let mut do_update = true;

        let use_sha1 = (finfo0.servicetype == FileService::OneDrive)
            || (finfo1.servicetype == FileService::OneDrive);
        let is_export = if use_sha1 {
            !(finfo0.sha1sum.is_some() && finfo1.sha1sum.is_some())
        } else {
            !(finfo0.md5sum.is_some() && finfo1.md5sum.is_some())
        };
        if finfo0.filename != finfo1.filename {
            return false;
        }
        if is_export {
            do_update = false;
        }
        if let Some(fstat0) = finfo0.filestat.as_ref() {
            if let Some(fstat1) = finfo1.filestat.as_ref() {
                if fstat0.st_mtime > fstat1.st_mtime {
                    do_update = true;
                }
                if fstat0.st_size != fstat1.st_size && !is_export {
                    do_update = true;
                }
            }
        }
        if use_sha1 {
            if let Some(sha0) = finfo0.sha1sum.as_ref() {
                if let Some(sha1) = finfo1.sha1sum.as_ref() {
                    if sha0 == sha1 {
                        return false;
                    }
                }
            }
        } else if let Some(md50) = finfo0.md5sum.as_ref() {
            if let Some(md51) = finfo1.md5sum.as_ref() {
                if md50 == md51 {
                    return false;
                }
            }
        }

        do_update
    }

    pub fn process_sync_cache(&self, pool: &PgPool) -> Result<(), Error> {
        let proc_list: Result<Vec<_>, Error> = FileSyncCache::get_cache_list(pool)?
            .into_par_iter()
            .map(|v| {
                let u0: Url = v.src_url.parse()?;
                let u1: Url = v.dst_url.parse()?;
                v.delete_cache_entry(pool)?;
                Ok((u0, u1))
            })
            .collect();

        let proc_map: HashMap<_, _> =
            proc_list?
                .into_iter()
                .fold(HashMap::new(), |mut h, (u0, u1)| {
                    let key = u0;
                    let val = u1;
                    h.entry(key).or_insert_with(Vec::new).push(val);
                    h
                });

        let key_list: Vec<_> = proc_map.keys().cloned().collect();

        for urls in group_urls(&key_list).values() {
            if let Some(u0) = urls.get(0) {
                let conf = FileListConf::from_url(u0, &self.config)?;
                let flist0 = FileList::from_conf(conf, pool.clone());
                let results: Result<Vec<_>, Error> = urls
                    .par_iter()
                    .map(|key| {
                        if let Some(vals) = proc_map.get(&key) {
                            for val in vals {
                                let finfo0 = match FileInfo::from_database(&pool, &key)? {
                                    Some(f) => f,
                                    None => FileInfo::from_url(&key)?,
                                };
                                let finfo1 = match FileInfo::from_database(&pool, &val)? {
                                    Some(f) => f,
                                    None => FileInfo::from_url(&val)?,
                                };
                                debug!("copy {} {}", key, val);
                                if finfo1.servicetype == FileService::Local {
                                    Self::copy_object(&flist0, &finfo0, &finfo1)?;
                                    flist0.cleanup()?;
                                } else {
                                    let conf = FileListConf::from_url(&val, &self.config)?;
                                    let flist1 = FileList::from_conf(conf, pool.clone());
                                    Self::copy_object(&flist1, &finfo0, &finfo1)?;
                                    flist1.cleanup()?;
                                }
                            }
                        }
                        Ok(())
                    })
                    .collect();
                results?;
            }
        }
        Ok(())
    }

    pub fn delete_files(&self, urls: &[Url], pool: &PgPool) -> Result<(), Error> {
        let all_urls: Vec<_> = if urls.is_empty() {
            let proc_list: Result<Vec<_>, Error> = FileSyncCache::get_cache_list(pool)?
                .into_par_iter()
                .map(|v| {
                    let u0: Url = v.src_url.parse()?;
                    let u1: Url = v.dst_url.parse()?;
                    Ok(vec![u0, u1])
                })
                .collect();
            proc_list?.into_par_iter().flatten().collect()
        } else {
            urls.to_vec()
        };

        group_urls(&all_urls)
            .values()
            .map(|urls| {
                let conf = FileListConf::from_url(&urls[0], &self.config)?;
                let flist = FileList::from_conf(conf, pool.clone());
                let fdict =
                    flist.get_file_list_dict(flist.load_file_list()?, FileInfoKeyType::UrlName);
                urls.into_par_iter()
                    .map(|url| {
                        let finfo = if let Some(f) = fdict.get(&url.as_str().to_string()) {
                            f.clone()
                        } else {
                            FileInfo::from_url(&url)?
                        };

                        debug!("delete {:?}", finfo);
                        flist.delete(&finfo)
                    })
                    .collect()
            })
            .collect()
    }

    pub fn copy_object<T, U, V>(flist: &T, finfo0: &U, finfo1: &V) -> Result<(), Error>
    where
        T: FileListTrait + Send + Sync + Debug,
        U: FileInfoTrait + Send + Sync + Debug,
        V: FileInfoTrait + Send + Sync + Debug,
    {
        let t0 = finfo0.get_finfo().servicetype;
        let t1 = finfo1.get_finfo().servicetype;

        debug!("copy from {:?} to {:?} using {:?}", t0, t1, flist);

        if t1 == FileService::Local {
            flist.copy_from(finfo0, finfo1)
        } else if t0 == FileService::Local {
            flist.copy_to(finfo0, finfo1)
        } else {
            Err(format_err!("Invalid request"))
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use rusoto_s3::{Object, Owner};
    use std::collections::HashMap;
    use std::env::current_dir;
    use std::io::{stdout, Write};
    use std::path::Path;

    use crate::config::Config;
    use crate::file_info::{FileInfoTrait, ServiceId, ServiceSession};
    use crate::file_info_local::FileInfoLocal;
    use crate::file_info_s3::FileInfoS3;
    use crate::file_list_local::{FileListLocal, FileListLocalConf};
    use crate::file_list_s3::{FileListS3, FileListS3Conf};
    use crate::file_sync::FileSync;
    use crate::models::FileSyncCache;
    use crate::pgpool::PgPool;

    #[test]
    fn test_compare_objects() -> Result<(), Error> {
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();
        let servicesession: ServiceSession = filepath.to_string_lossy().parse()?;
        let finfo0 = FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession))?;
        writeln!(stdout(), "{:?}", finfo0)?;
        let mut finfo1 = finfo0.clone();
        finfo1.0.md5sum = Some("51e3cc2c6f64d24ff55fae262325edee".parse()?);
        let mut fstat = finfo1.0.filestat.unwrap();
        fstat.st_mtime += 100;
        fstat.st_size += 100;
        finfo1.0.filestat = Some(fstat);
        writeln!(stdout(), "{:?}", finfo1)?;
        assert!(FileSync::compare_objects(&finfo0, &finfo1));

        let test_owner = Owner {
            display_name: Some("me".to_string()),
            id: Some("8675309".to_string()),
        };
        let test_object = Object {
            e_tag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.to_string()),
            key: Some("src/file_sync.rs".to_string()),
            last_modified: Some("2019-05-01T00:00:00+00:00".to_string()),
            owner: Some(test_owner),
            size: Some(100),
            storage_class: Some("Standard".to_string()),
        };

        let finfo2 = FileInfoS3::from_object("test_bucket", test_object)?;
        writeln!(stdout(), "{:?}", finfo2)?;
        assert!(FileSync::compare_objects(&finfo0, &finfo2));
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_compare_lists_0() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();
        let servicesession: ServiceSession = filepath.to_string_lossy().parse()?;
        let finfo0 = FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession))?;
        writeln!(stdout(), "{:?}", finfo0)?;

        let flist0conf = FileListLocalConf::new(&current_dir()?, &config)?;
        let flist0 = FileListLocal::from_conf(flist0conf, pool.clone()).with_list(vec![finfo0.0]);

        let flist1conf = FileListS3Conf::new("test_bucket", &config)?;
        let flist1 = FileListS3::from_conf(flist1conf, pool.clone());

        FileSync::compare_lists(&flist0, &flist1, &pool)?;

        let cache_list: HashMap<_, _> = FileSyncCache::get_cache_list(&pool)?
            .into_iter()
            .filter(|v| v.src_url.starts_with("file://"))
            .map(|v| (v.src_url.clone(), v))
            .collect();

        assert!(cache_list.len() > 0);

        writeln!(stdout(), "{:?}", cache_list)?;

        let test_key = format!(
            "file://{}/src/file_sync.rs",
            current_dir()?.to_string_lossy()
        );
        assert!(cache_list.contains_key(&test_key));

        for val in cache_list.values() {
            val.delete_cache_entry(&pool)?;
        }
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_compare_lists_1() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();
        let servicesession: ServiceSession = filepath.to_string_lossy().parse()?;

        let finfo0 = FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession))?;
        writeln!(stdout(), "{:?}", finfo0)?;

        let flist0conf = FileListLocalConf::new(&current_dir()?, &config)?;
        let flist0 = FileListLocal::from_conf(flist0conf, pool.clone());

        let test_owner = Owner {
            display_name: Some("me".to_string()),
            id: Some("8675309".to_string()),
        };
        let test_object = Object {
            e_tag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.to_string()),
            key: Some("src/file_sync.rs".to_string()),
            last_modified: Some("2019-05-01T00:00:00+00:00".to_string()),
            owner: Some(test_owner),
            size: Some(100),
            storage_class: Some("Standard".to_string()),
        };

        let finfo1 = FileInfoS3::from_object("test_bucket", test_object)?;

        let flist1conf = FileListS3Conf::new("test_bucket", &config)?;
        let flist1 = FileListS3::from_conf(flist1conf, pool.clone()).with_list(vec![finfo1.into_finfo()]);

        FileSync::compare_lists(&flist0, &flist1, &pool)?;

        let cache_list: HashMap<_, _> = FileSyncCache::get_cache_list(&pool)?
            .into_iter()
            .filter(|v| v.src_url.starts_with("s3://"))
            .map(|v| (v.src_url.clone(), v))
            .collect();

        assert!(cache_list.len() > 0);

        writeln!(stdout(), "{:?}", cache_list)?;

        let test_key = "s3://test_bucket/src/file_sync.rs".to_string();
        assert!(cache_list.contains_key(&test_key));

        for val in cache_list.values() {
            val.delete_cache_entry(&pool)?;
        }
        Ok(())
    }
}
