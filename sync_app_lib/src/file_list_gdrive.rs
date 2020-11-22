use anyhow::{format_err, Error};
use async_trait::async_trait;
use log::debug;
use parking_lot::RwLock;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::HashMap,
    fs::create_dir_all,
    io::{stdout, Write},
    path::Path,
    sync::Arc,
};
use tokio::task::spawn_blocking;
use url::Url;

use gdrive_lib::{
    directory_info::DirectoryInfo,
    gdrive_instance::{GDriveInfo, GDriveInstance},
};

use stack_string::StackString;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession},
    file_info_gdrive::FileInfoGDrive,
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    pgpool::PgPool,
};

#[derive(Debug, Clone)]
pub struct FileListGDrive {
    pub flist: FileList,
    pub gdrive: GDriveInstance,
    pub directory_map: Arc<RwLock<HashMap<StackString, DirectoryInfo>>>,
    pub root_directory: Arc<RwLock<Option<StackString>>>,
}

impl FileListGDrive {
    pub fn new(
        servicesession: &str,
        basepath: &str,
        config: &Config,
        pool: &PgPool,
    ) -> Result<Self, Error> {
        let baseurl: Url = format!("gdrive://{}/{}", servicesession, basepath).parse()?;
        let basepath = Path::new(basepath);

        let flist = FileList::new(
            baseurl,
            basepath.to_path_buf(),
            config.clone(),
            FileService::GDrive,
            servicesession.parse()?,
            HashMap::new(),
            pool.clone(),
        );

        let gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            &flist.servicesession.0,
        );

        Ok(Self {
            flist,
            gdrive,
            directory_map: Arc::new(RwLock::new(HashMap::new())),
            root_directory: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "gdrive" {
            let servicesession = url
                .as_str()
                .trim_start_matches("gdrive://")
                .replace(url.path(), "");
            let tmp = format!("gdrive://{}/", servicesession);
            let basepath: Url = url.as_str().replace(&tmp, "file:///").parse()?;
            let basepath = basepath
                .to_file_path()
                .map_err(|e| format_err!("Failure {:?}", e))?;
            let basepath = basepath.to_string_lossy().to_string();
            let basepath = Path::new(basepath.trim_start_matches('/'));
            let flist = FileList::new(
                url.clone(),
                basepath.to_path_buf(),
                config.clone(),
                FileService::GDrive,
                servicesession.parse()?,
                HashMap::new(),
                pool.clone(),
            );

            let config = config.clone();
            let servicesession = flist.servicesession.0.clone();
            let gdrive = spawn_blocking(move || {
                GDriveInstance::new(
                    &config.gdrive_token_path,
                    &config.gdrive_secret_file,
                    &servicesession,
                )
            })
            .await?;

            Ok(Self {
                flist,
                gdrive,
                directory_map: Arc::new(RwLock::new(HashMap::new())),
                root_directory: Arc::new(RwLock::new(None)),
            })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }

    pub fn set_directory_map(&self, use_cache: bool) -> Result<(), Error> {
        let (dmap, root_dir) = if use_cache {
            let dlist = self.load_directory_info_cache()?;
            self.get_directory_map_cache(dlist)
        } else {
            self.gdrive.get_directory_map()?
        };
        if !use_cache {
            self.clear_directory_list()?;
            self.cache_directory_map(&dmap, &root_dir)?;
        }
        *self.directory_map.write() = dmap;
        *self.root_directory.write() = root_dir;

        Ok(())
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.gdrive = self.gdrive.with_max_keys(max_keys);
        self
    }

    pub fn set_root_directory(&self, root_directory: &str) {
        self.root_directory.write().replace(root_directory.into());
    }

    fn convert_gdriveinfo_to_file_info(
        &self,
        flist: &[GDriveInfo],
    ) -> Result<Vec<FileInfo>, Error> {
        let flist: Result<Vec<_>, Error> = flist
            .par_iter()
            .map(|f| FileInfoGDrive::from_gdriveinfo(f.clone()).map(FileInfoTrait::into_finfo))
            .collect();
        let flist = flist?
            .into_par_iter()
            .filter(|f| {
                if let Some(url) = f.urlname.as_ref() {
                    if url.as_str().contains(self.get_baseurl().as_str()) {
                        return true;
                    }
                }
                false
            })
            .map(|f| {
                let mut inner = f.inner().clone();
                inner
                    .servicesession
                    .replace(self.get_servicesession().clone());
                FileInfo::from_inner(inner)
            })
            .collect();
        Ok(flist)
    }

    fn get_all_files(&self) -> Result<Vec<FileInfo>, Error> {
        let flist: Vec<_> = self
            .gdrive
            .get_all_file_info(false, &self.directory_map.read())?;

        let flist = self.convert_gdriveinfo_to_file_info(&flist)?;

        Ok(flist)
    }

    fn get_all_changes(&self) -> Result<(Vec<StackString>, Vec<FileInfo>), Error> {
        let chlist: Vec<_> = self.gdrive.get_all_changes()?;
        let delete_list = chlist
            .iter()
            .filter_map(|ch| match ch.file {
                Some(_) => None,
                None => ch.file_id.clone().map(Into::into),
            })
            .collect();
        let flist: Vec<_> = chlist.into_iter().filter_map(|ch| ch.file).collect();

        let flist = self
            .gdrive
            .convert_file_list_to_gdrive_info(&flist, &self.directory_map.read())?;
        let flist = self.convert_gdriveinfo_to_file_info(&flist)?;
        Ok((delete_list, flist))
    }
}

#[async_trait]
impl FileListTrait for FileListGDrive {
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
        self.flist.with_list(filelist)
    }

    #[allow(clippy::similar_names)]
    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(false)?;
            let start_page_token = glist.gdrive.get_start_page_token()?;
            let file_list = glist.load_file_list()?;
            let mut flist_dict =
                { glist.get_file_list_dict(&file_list, FileInfoKeyType::ServiceId) };

            let (dlist, flist) = if glist.gdrive.start_page_token.is_some() {
                glist.get_all_changes()?
            } else {
                {
                    glist.clear_file_list()?;
                    (Vec::new(), glist.get_all_files()?)
                }
            };

            debug!("delete {} insert {}", dlist.len(), flist.len());

            for dfid in &dlist {
                flist_dict.remove(dfid);
            }

            for f in flist {
                if let Some(fid) = f.serviceid.as_ref() {
                    flist_dict.insert(fid.0.clone(), f);
                }
            }

            let flist = flist_dict.into_iter().map(|(_, v)| v).collect();

            let gdrive = glist.gdrive.clone().with_start_page_token(start_page_token);
            let ext = glist
                .gdrive
                .start_page_token_filename
                .extension()
                .ok_or_else(|| format_err!("No ext"))?
                .to_string_lossy();
            let start_page_path = glist
                .gdrive
                .start_page_token_filename
                .with_extension(format!("{}.new", ext));
            gdrive.store_start_page_token(&start_page_path)?;

            Ok(flist)
        })
        .await?
    }

    async fn print_list(&self) -> Result<(), Error> {
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(false)?;
            let dnamemap = GDriveInstance::get_directory_name_map(&glist.directory_map.read());
            let parents = if let Ok(Some(p)) =
                GDriveInstance::get_parent_id(&glist.get_baseurl(), &dnamemap)
            {
                Some(vec![p])
            } else if let Some(root_dir) = glist.root_directory.read().as_ref() {
                Some(vec![root_dir.clone()])
            } else {
                None
            };
            glist.gdrive.process_list_of_keys(&parents, |i| {
                if let Ok(finfo) =
                    GDriveInfo::from_object(i, &glist.gdrive, &glist.directory_map.read())
                        .and_then(FileInfoGDrive::from_gdriveinfo)
                {
                    if let Some(url) = finfo.get_finfo().urlname.as_ref() {
                        writeln!(stdout().lock(), "{}", url.as_str())?;
                    }
                }
                Ok(())
            })
        })
        .await?
    }

    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo().clone();
        let finfo1 = finfo1.get_finfo().clone();
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(true)?;
            if finfo0.servicetype == FileService::GDrive && finfo1.servicetype == FileService::Local
            {
                let local_path = finfo1
                    .filepath
                    .as_ref()
                    .ok_or_else(|| format_err!("No local path"))?
                    .clone();
                let parent_dir = finfo1
                    .filepath
                    .as_ref()
                    .ok_or_else(|| format_err!("No local path"))?
                    .parent()
                    .ok_or_else(|| format_err!("No parent directory"))?;
                if !parent_dir.exists() {
                    create_dir_all(&parent_dir)?;
                }
                let gdriveid = finfo0
                    .serviceid
                    .clone()
                    .ok_or_else(|| format_err!("No gdrive url"))?
                    .0;
                let gfile = glist.gdrive.get_file_metadata(&gdriveid)?;
                debug!("{:?}", gfile.mime_type);
                if GDriveInstance::is_unexportable(&gfile.mime_type) {
                    debug!("unexportable");
                    glist.remove_by_id(&gdriveid)?;
                    debug!("removed from database");
                    return Ok(());
                }
                glist
                    .gdrive
                    .download(&gdriveid, &local_path, &gfile.mime_type)
            } else {
                Err(format_err!(
                    "Invalid types {} {}",
                    finfo0.servicetype,
                    finfo1.servicetype
                ))
            }
        })
        .await?
    }

    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo().clone();
        let finfo1 = finfo1.get_finfo().clone();
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(true)?;
            if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::GDrive
            {
                let local_file = finfo0
                    .filepath
                    .clone()
                    .ok_or_else(|| format_err!("No local path"))?
                    .canonicalize()?;
                let local_url =
                    Url::from_file_path(local_file).map_err(|e| format_err!("failure {:?}", e))?;

                let remote_url = finfo1
                    .urlname
                    .clone()
                    .ok_or_else(|| format_err!("No remote url"))?;
                let dnamemap = GDriveInstance::get_directory_name_map(&glist.directory_map.read());
                let parent_id = GDriveInstance::get_parent_id(&remote_url, &dnamemap)?
                    .ok_or_else(|| format_err!("No parent id!"))?;
                glist.gdrive.upload(&local_url, &parent_id)?;
                Ok(())
            } else {
                Err(format_err!(
                    "Invalid types {} {}",
                    finfo0.servicetype,
                    finfo1.servicetype
                ))
            }
        })
        .await?
    }

    async fn move_file(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo().clone();
        let finfo1 = finfo1.get_finfo().clone();
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(true)?;
            if finfo0.servicetype != finfo1.servicetype
                || glist.get_servicetype() != finfo0.servicetype
            {
                return Ok(());
            }
            let gdriveid = &finfo0
                .serviceid
                .as_ref()
                .ok_or_else(|| format_err!("No serviceid"))?
                .0;
            let url = finfo1
                .urlname
                .as_ref()
                .ok_or_else(|| format_err!("No url"))?;
            let dnamemap = GDriveInstance::get_directory_name_map(&glist.directory_map.read());
            let parentid = GDriveInstance::get_parent_id(&url, &dnamemap)?
                .ok_or_else(|| format_err!("No parentid"))?;
            glist.gdrive.move_to(gdriveid, &parentid, &finfo1.filename)
        })
        .await?
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo().clone();
        let glist = self.clone();
        spawn_blocking(move || {
            glist.set_directory_map(true)?;
            if finfo.servicetype == FileService::GDrive {
                finfo.serviceid.as_ref().map_or(Ok(()), |gdriveid| {
                    glist.gdrive.delete_permanently(&gdriveid.0).map(|_| ())
                })
            } else {
                Err(format_err!("Wrong service type"))
            }
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{format_err, Error};
    use chrono::NaiveDate;
    use log::{debug, error};
    use std::{
        collections::HashMap,
        fs::{create_dir_all, File},
        io::{BufRead, BufReader, Write},
        path::{Path, PathBuf},
    };
    use tokio::fs::{copy, remove_file, rename};
    use walkdir::WalkDir;

    use gdrive_lib::gdrive_instance::GDriveInstance;

    use crate::{
        config::Config, file_list::FileListTrait, file_list_gdrive::FileListGDrive, pgpool::PgPool,
    };

    struct TempStartPageToken {
        new: PathBuf,
    }

    impl TempStartPageToken {
        async fn new(fname: &Path) -> Result<Self, Error> {
            let original = fname.to_path_buf();
            let ext = original.extension().unwrap().to_string_lossy();
            let new = fname.with_extension(format!("{}.new", ext)).to_path_buf();

            if new.exists() {
                remove_file(&new).await?;
            }
            if original.exists() {
                remove_file(&original).await?;
            }
            Ok(Self { new })
        }

        async fn cleanup(&self) -> Result<(), Error> {
            if self.new.exists() {
                remove_file(&self.new).await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_gdrive_fill_file_list() -> Result<(), Error> {
        let config = Config::init_config()?;

        let fname = config
            .gdrive_token_path
            .join(format!("{}_start_page_token", "ddboline@gmail.com"));
        let tmp = TempStartPageToken::new(&fname).await?;

        let pool = PgPool::new(&config.database_url);

        let mut flist =
            FileListGDrive::new("ddboline@gmail.com", "My Drive", &config, &pool)?.max_keys(100);
        flist.set_directory_map(false)?;

        let new_flist = flist.fill_file_list().await?;

        assert!(new_flist.len() > 0);

        flist.clear_file_list()?;

        flist.with_list(new_flist);

        let result = flist.cache_file_list()?;
        debug!("wrote {}", result);

        let new_flist = flist.load_file_list()?;

        assert_eq!(flist.flist.get_filemap().len(), new_flist.len());

        flist.clear_file_list()?;

        debug!("dmap {}", flist.directory_map.read().len());

        let dnamemap = GDriveInstance::get_directory_name_map(&flist.directory_map.read());
        for f in flist.get_filemap().values() {
            let u = f.urlname.as_ref().unwrap();
            let parent_id = GDriveInstance::get_parent_id(u, &dnamemap)?;
            assert!(!parent_id.is_none());
            debug!("{} {:?}", u, parent_id);
        }

        let multimap: HashMap<_, _> = dnamemap.iter().filter(|(_, v)| v.len() > 1).collect();
        debug!("multimap {}", multimap.len());
        for (key, val) in &multimap {
            if val.len() > 1 {
                debug!("{} {}", key, val.len());
            }
        }

        tmp.cleanup().await?;

        let mut flist = FileListGDrive::new("ddboline@gmail.com", "My Drive", &config, &pool)?;
        flist.set_directory_map(false)?;

        let new_flist = flist.fill_file_list().await?;
        assert!(new_flist.len() > 0);
        flist.with_list(new_flist);
        let result = flist.cache_file_list()?;
        debug!("wrote {}", result);
        flist.cleanup()?;

        Ok(())
    }
}
