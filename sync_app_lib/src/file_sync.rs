use anyhow::{format_err, Error};
use fmt::Debug;
use futures::{future::try_join_all, TryStreamExt};
use log::debug;
use smallvec::{smallvec, SmallVec};
use std::{
    collections::HashMap,
    convert::{From, TryInto},
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use url::Url;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, FileStat},
    file_list::{group_urls, replace_basepath, replace_baseurl, FileList, FileListTrait},
    file_service::FileService,
    models::{CandidateIds, FileInfoCache, FileSyncCache},
    pgpool::PgPool,
};

#[derive(Debug, PartialEq, Clone, Copy, Eq)]
pub enum FileSyncAction {
    Index,
    Sync,
    Process,
    Copy,
    List,
    Delete,
    Move,
    Count,
    Serialize,
    AddConfig,
    ShowConfig,
    ShowCache,
    SyncGarmin,
    SyncMovie,
    SyncCalendar,
    SyncSecurity,
    SyncWeather,
    SyncAll,
    RunMigrations,
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
            "count" => Ok(Self::Count),
            "ser" | "serialize" => Ok(Self::Serialize),
            "add" | "add_config" => Ok(Self::AddConfig),
            "show_config" => Ok(Self::ShowConfig),
            "show" | "show_cache" => Ok(Self::ShowCache),
            "sync_garmin" => Ok(Self::SyncGarmin),
            "sync_movie" => Ok(Self::SyncMovie),
            "sync_calendar" => Ok(Self::SyncCalendar),
            "sync_security" => Ok(Self::SyncSecurity),
            "sync_weather" => Ok(Self::SyncWeather),
            "sync_all" => Ok(Self::SyncAll),
            "run-migrations" => Ok(Self::RunMigrations),
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
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn compare_lists(
        flist0: &dyn FileListTrait,
        flist1: &dyn FileListTrait,
        pool: &PgPool,
    ) -> Result<(), Error> {
        let count0 = FileInfoCache::count_cached(
            flist0.get_servicesession().as_str(),
            flist0.get_servicetype().to_str(),
            pool,
            false,
        )
        .await?;
        let count1 = FileInfoCache::count_cached(
            flist1.get_servicesession().as_str(),
            flist1.get_servicetype().to_str(),
            pool,
            false,
        )
        .await?;
        debug!(
            "f0 {} {count0} f1 {} {count1}",
            flist0.get_baseurl(),
            flist1.get_baseurl(),
        );
        let mut list_a_not_b: Vec<(FileInfo, FileInfo)> = Vec::new();
        let mut list_b_not_a: Vec<(FileInfo, FileInfo)> = Vec::new();

        for finfo0 in FileInfoCache::get_new_entries(
            flist0.get_baseurl().as_str(),
            flist1.get_baseurl().as_str(),
            flist0.get_servicesession().as_str(),
            pool,
        )
        .await?
        {
            let path0 = Path::new(&finfo0.filepath);
            let url0 = &finfo0.urlname.parse()?;
            let baseurl0 = flist0.get_baseurl();
            let baseurl1 = flist1.get_baseurl();
            let url1 = replace_baseurl(url0, baseurl0, baseurl1)?;
            let path1 = replace_basepath(path0, flist0.get_basepath(), flist1.get_basepath());
            if !url1.as_str().contains(baseurl1.as_str()) {
                return Err(format_err!("{baseurl1} not in {url1}"));
            }
            let finfo0: FileInfo = finfo0.try_into()?;
            let finfo1: FileInfo = FileInfo::new(
                finfo0.filename.clone(),
                path1.into(),
                url1.into(),
                None,
                None,
                FileStat::default(),
                flist1.get_servicesession().clone().into(),
                flist1.get_servicetype(),
                flist1.get_servicesession().clone(),
            );
            debug!("ab {} {}", finfo0.urlname, finfo1.urlname);
            list_a_not_b.push((finfo0, finfo1));
        }

        let candidates: Vec<_> = FileInfoCache::get_copy_candidates(
            flist0.get_baseurl().as_str(),
            flist1.get_baseurl().as_str(),
            flist0.get_servicesession().as_str(),
            flist1.get_servicesession().as_str(),
            pool,
        )
        .await?
        .try_collect()
        .await?;

        for CandidateIds { f0id, f1id } in candidates {
            if let Some(finfo0) = FileInfoCache::get_by_id(f0id, pool).await? {
                if let Some(finfo1) = FileInfoCache::get_by_id(f1id, pool).await? {
                    let finfo0: FileInfo = finfo0.try_into()?;
                    let finfo1: FileInfo = finfo1.try_into()?;
                    if Self::compare_objects(&finfo0, &finfo1) {
                        list_a_not_b.push((finfo0, finfo1));
                    }
                }
            }
        }

        for finfo1 in FileInfoCache::get_new_entries(
            flist1.get_baseurl().as_str(),
            flist0.get_baseurl().as_str(),
            flist1.get_servicesession().as_str(),
            pool,
        )
        .await?
        {
            let path1 = Path::new(&finfo1.filepath);
            let url1 = &finfo1.urlname.parse()?;
            let baseurl0 = flist0.get_baseurl();
            let baseurl1 = flist1.get_baseurl();
            let url0 = replace_baseurl(url1, baseurl1, baseurl0)?;
            let path0 = replace_basepath(path1, flist1.get_basepath(), flist0.get_basepath());
            if !url0.as_str().contains(baseurl0.as_str()) {
                return Err(format_err!("{baseurl0} not in {url1}"));
            }
            let finfo0 = FileInfo::new(
                finfo1.filename.clone(),
                path0.into(),
                url0.into(),
                None,
                None,
                FileStat::default(),
                flist0.get_servicesession().clone().into(),
                flist0.get_servicetype(),
                flist0.get_servicesession().clone(),
            );
            let finfo1: FileInfo = finfo1.try_into()?;
            debug!("ba {finfo0:?} {finfo1:?}",);
            list_b_not_a.push((finfo1, finfo0));
        }
        debug!("ab {} ba {}", list_a_not_b.len(), list_b_not_a.len());
        if list_a_not_b.is_empty() && list_b_not_a.is_empty() {
            flist0.cleanup().and_then(|()| flist1.cleanup())
        } else {
            for (f0, f1) in list_a_not_b.into_iter().chain(list_b_not_a.into_iter()) {
                FileSyncCache::cache_sync(pool, f0.urlname.as_str(), f1.urlname.as_str()).await?;
            }
            Ok(())
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
        if finfo0.filestat.st_mtime > finfo1.filestat.st_mtime {
            do_update = true;
        }
        if finfo0.filestat.st_size != finfo1.filestat.st_size && !is_export {
            do_update = true;
        }
        if finfo0.filestat.st_size == finfo1.filestat.st_size && !is_export {
            do_update = false;
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

    /// # Errors
    /// Return error if db query fails
    pub async fn process_sync_cache(&self, pool: &PgPool) -> Result<(), Error> {
        let proc_map: Result<HashMap<_, _>, Error> = FileSyncCache::get_cache_list(pool)
            .await?
            .map_err(Into::into)
            .try_fold(HashMap::new(), |mut h: HashMap<_, Vec<_>>, v| async move {
                let u0: Url = v.src_url.parse()?;
                let u1: Url = v.dst_url.parse()?;
                v.delete_cache_entry(pool).await?;
                h.entry(u0).or_default().push(u1);
                Ok(h)
            })
            .await;
        let proc_map = Arc::new(proc_map?);

        let key_list: Vec<_> = proc_map.keys().cloned().collect();

        for urls in group_urls(&key_list).values() {
            if let Some(u0) = urls.first() {
                let futures = urls.iter().map(|key| {
                    let key = key.clone();
                    let proc_map = proc_map.clone();
                    let u0 = u0.clone();
                    async move {
                        if let Some(vals) = proc_map.get(&key) {
                            let flist0 = FileList::from_url(&u0, &self.config, pool).await?;
                            for val in vals {
                                let flist1 = FileList::from_url(val, &self.config, pool).await?;
                                let finfo0 = match FileInfo::from_database(
                                    pool,
                                    &key,
                                    flist0.get_servicesession().as_str(),
                                )
                                .await?
                                {
                                    Some(f) => f,
                                    None => FileInfo::from_url(&key)?,
                                };
                                let finfo1 = match FileInfo::from_database(
                                    pool,
                                    val,
                                    flist1.get_servicesession().as_str(),
                                )
                                .await?
                                {
                                    Some(f) => f,
                                    None => FileInfo::from_url(val)?,
                                };
                                debug!("copy {key} {val}",);
                                if finfo1.servicetype == FileService::Local {
                                    Self::copy_object(&(*flist0), &finfo0, &finfo1).await?;
                                    flist0.cleanup()?;
                                } else {
                                    Self::copy_object(&(*flist1), &finfo0, &finfo1).await?;
                                    flist1.cleanup()?;
                                }
                            }
                        }
                        Ok(())
                    }
                });
                let result: Result<Vec<()>, Error> = try_join_all(futures).await;
                result?;
            }
        }
        Ok(())
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn delete_files(&self, urls: &[Url], pool: &PgPool) -> Result<(), Error> {
        let all_urls: Vec<Url> = if urls.is_empty() {
            let proc_list: Result<Vec<SmallVec<[Url; 2]>>, Error> =
                FileSyncCache::get_cache_list(pool)
                    .await?
                    .map_err(Into::into)
                    .and_then(|v| async move {
                        let u0: Url = v.src_url.parse()?;
                        let u1: Url = v.dst_url.parse()?;
                        Ok(smallvec![u0, u1])
                    })
                    .try_collect()
                    .await;
            proc_list?.into_iter().flatten().collect()
        } else {
            urls.to_vec()
        };

        for urls in group_urls(&all_urls).values() {
            let flist = Arc::new(FileList::from_url(&urls[0], &self.config, pool).await?);
            let fdict = Arc::new(flist.get_file_list_dict(
                &flist.load_file_list(false).await?,
                FileInfoKeyType::UrlName,
            ));

            let futures = urls.iter().map(|url| {
                let flist = flist.clone();
                let fdict = fdict.clone();
                async move {
                    let finfo = if let Some(f) = fdict.get(url.as_str()) {
                        f.clone()
                    } else {
                        FileInfo::from_url(url)?
                    };

                    debug!("delete {finfo:?}",);
                    flist.delete(&finfo).await
                }
            });
            let results: Result<Vec<()>, Error> = try_join_all(futures).await;
            results?;
        }
        Ok(())
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn copy_object(
        flist: &dyn FileListTrait,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let t0 = finfo0.get_finfo().servicetype;
        let t1 = finfo1.get_finfo().servicetype;

        debug!("copy from {t0:?} to {t1:?} using {flist:?}",);

        if t1 == FileService::Local {
            flist.copy_from(finfo0, finfo1).await
        } else if t0 == FileService::Local {
            flist.copy_to(finfo0, finfo1).await
        } else {
            Err(format_err!("Invalid request"))
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use aws_sdk_s3::{
        primitives::DateTime,
        types::{Object, ObjectStorageClass, Owner},
    };
    use futures::{future, TryStreamExt};
    use log::debug;
    use stack_string::format_sstr;
    use std::{collections::HashMap, convert::TryInto, env::current_dir, path::Path};
    use time::macros::datetime;

    use crate::{
        config::Config,
        file_info::{FileInfo, FileInfoTrait, ServiceId, ServiceSession},
        file_info_local::FileInfoLocal,
        file_info_s3::FileInfoS3,
        file_list::FileListTrait,
        file_list_local::FileListLocal,
        file_list_s3::FileListS3,
        file_sync::FileSync,
        models::{FileInfoCache, FileSyncCache},
        pgpool::PgPool,
    };

    #[test]
    fn test_compare_objects() -> Result<(), Error> {
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();
        let servicesession: ServiceSession = filepath.to_string_lossy().parse()?;
        let finfo0 = FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession))?;
        debug!("{:?}", finfo0);
        let mut finfo1 = finfo0.0.inner().clone();
        finfo1.md5sum = Some("51e3cc2c6f64d24ff55fae262325edee".parse()?);
        finfo1.filestat.st_mtime += 100;
        finfo1.filestat.st_size += 100;
        let finfo1 = FileInfoLocal(FileInfo::from_inner(finfo1));
        debug!("{:?}", finfo1);
        assert!(FileSync::compare_objects(&finfo0, &finfo1));

        let test_owner = Owner::builder().display_name("me").id("8675309").build();
        let last_modified = datetime!(2019-05-01 00:00:00 +00:00);
        let test_object = Object::builder()
            .e_tag(r#""6f90ebdaabef92a9f76be131037f593b""#)
            .key("src/file_sync.rs")
            .last_modified(DateTime::from_secs(last_modified.unix_timestamp()))
            .owner(test_owner)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .build();

        let finfo2 = FileInfoS3::from_object("test_bucket", test_object)?;
        debug!("{:?}", finfo2);
        assert!(FileSync::compare_objects(&finfo0, &finfo2));
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_compare_lists_0() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();

        let flist0 = FileListLocal::new(&current_dir()?, &config, &pool)?;
        flist0.clear_file_list().await?;

        let finfo0 = FileInfoLocal::from_path(
            &filepath,
            Some(serviceid),
            Some(flist0.get_servicesession().clone()),
        )?;
        debug!(
            "{} {}",
            finfo0.get_finfo().servicesession.as_str(),
            flist0.get_servicesession().as_str()
        );
        let finfo0: FileInfoCache = finfo0.get_finfo().try_into()?;
        finfo0.insert(&pool).await?;

        let flist1 = FileListS3::new("test_bucket", &config, &pool).await?;
        flist1.clear_file_list().await?;

        FileSync::compare_lists(&flist0, &flist1, &pool).await?;

        let cache_list: HashMap<_, _> = FileSyncCache::get_cache_list(&pool)
            .await?
            .try_filter(|v| future::ready(v.src_url.starts_with("file://")))
            .map_ok(|v| (v.src_url.clone(), v))
            .try_collect()
            .await?;

        assert!(cache_list.len() > 0);

        debug!("{:?}", cache_list);

        let test_key = format_sstr!(
            "file://{}/src/file_sync.rs",
            current_dir()?.to_string_lossy()
        );
        assert!(cache_list.contains_key(test_key.as_str()));

        for val in cache_list.values() {
            val.delete_cache_entry(&pool).await?;
        }
        finfo0.delete(&pool).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_compare_lists_1() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;
        let filepath = Path::new("src/file_sync.rs").canonicalize()?;
        let serviceid: ServiceId = filepath.to_string_lossy().to_string().into();

        let flist0 = FileListLocal::new(&current_dir()?, &config, &pool)?;
        flist0.clear_file_list().await?;

        let finfo0 = FileInfoLocal::from_path(
            &filepath,
            Some(serviceid),
            Some(flist0.get_servicesession().clone()),
        )?;
        let finfo0: FileInfoCache = finfo0.get_finfo().try_into()?;
        debug!("{:?}", finfo0);
        finfo0.insert(&pool).await?;

        let test_owner = Owner::builder().display_name("me").id("8675309").build();
        let last_modified = datetime!(2019-05-01 00:00:00 +00:00);
        let test_object = Object::builder()
            .e_tag(r#""6f90ebdaabef92a9f76be131037f593b""#)
            .key("src/file_sync.rs")
            .last_modified(DateTime::from_secs(last_modified.unix_timestamp()))
            .owner(test_owner)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .build();

        let finfo1 = FileInfoS3::from_object("test_bucket", test_object)?;
        let finfo1: FileInfoCache = finfo1.get_finfo().try_into()?;
        finfo1.insert(&pool).await?;

        let flist1 = FileListS3::new("test_bucket", &config, &pool).await?;

        FileSync::compare_lists(&flist0, &flist1, &pool).await?;

        let cache_list: HashMap<_, _> = FileSyncCache::get_cache_list(&pool)
            .await?
            .try_filter(|v| future::ready(v.dst_url.starts_with("s3://")))
            .map_ok(|v| (v.dst_url.clone(), v))
            .try_collect()
            .await?;

        assert!(cache_list.len() > 0);

        debug!("{:?}", cache_list);

        let test_key = "s3://test_bucket/src/file_sync.rs".to_string();
        assert!(cache_list.contains_key(test_key.as_str()));

        for val in cache_list.values() {
            val.delete_cache_entry(&pool).await?;
        }
        finfo0.delete(&pool).await?;
        finfo1.delete(&pool).await?;

        Ok(())
    }
}
