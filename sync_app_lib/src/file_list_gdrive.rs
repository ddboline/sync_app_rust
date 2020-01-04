use failure::{err_msg, format_err, Error};
use log::debug;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::io::{stdout, Write};
use std::path::Path;
use std::sync::Arc;
use url::Url;

use gdrive_lib::directory_info::DirectoryInfo;
use gdrive_lib::gdrive_instance::{GDriveInfo, GDriveInstance};

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait};
use crate::file_info_gdrive::FileInfoGDrive;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;

#[derive(Debug, Clone)]
pub struct FileListGDrive {
    pub flist: FileList,
    pub gdrive: GDriveInstance,
    pub directory_map: Arc<HashMap<String, DirectoryInfo>>,
    pub root_directory: Option<String>,
    pub pool: Option<PgPool>,
}

#[derive(Debug, Clone)]
pub struct FileListGDriveConf(pub FileListConf);

impl FileListGDrive {
    pub fn from_conf(
        conf: FileListGDriveConf,
        gdrive: &GDriveInstance,
        pool: Option<&PgPool>,
    ) -> Result<FileListGDrive, Error> {
        let f = FileListGDrive {
            flist: FileList::from_conf(conf.0),
            gdrive: gdrive.clone(),
            directory_map: Arc::new(HashMap::new()),
            root_directory: None,
            pool: pool.cloned(),
        };
        Ok(f)
    }

    pub fn set_directory_map(
        mut self,
        use_cache: bool,
        pool: Option<&PgPool>,
    ) -> Result<Self, Error> {
        let (dmap, root_dir) = if use_cache && pool.is_some() {
            match pool {
                Some(pool) => {
                    let dlist = self.load_directory_info_cache(&pool)?;
                    self.get_directory_map_cache(dlist)
                }
                _ => unreachable!(),
            }
        } else {
            self.gdrive.get_directory_map()?
        };
        if !use_cache {
            if let Some(pool) = pool {
                self.clear_directory_list(&pool)?;
                self.cache_directory_map(&pool, &dmap, &root_dir)?;
            }
        }
        self.directory_map = Arc::new(dmap);
        self.root_directory = root_dir;

        Ok(self)
    }

    pub fn with_list(self, filelist: Vec<FileInfo>) -> FileListGDrive {
        FileListGDrive {
            flist: self.flist.with_list(filelist),
            gdrive: self.gdrive,
            directory_map: self.directory_map,
            root_directory: self.root_directory,
            pool: self.pool,
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.gdrive = self.gdrive.with_max_keys(max_keys);
        self
    }

    pub fn set_root_directory(mut self, root_directory: &str) -> Self {
        self.root_directory = Some(root_directory.to_string());
        self
    }

    fn convert_gdriveinfo_to_file_info(
        &self,
        flist: &[GDriveInfo],
    ) -> Result<Vec<FileInfo>, Error> {
        let flist: Result<Vec<_>, Error> = flist
            .par_iter()
            .map(|f| FileInfoGDrive::from_gdriveinfo(f.clone()).map(FileInfoTrait::into_finfo))
            .collect();
        let flist: Vec<_> = flist?
            .into_par_iter()
            .filter(|f| {
                if let Some(url) = f.urlname.as_ref() {
                    if url.as_str().contains(self.get_conf().baseurl.as_str()) {
                        return true;
                    }
                }
                false
            })
            .map(|mut f| {
                f.servicesession
                    .replace(self.get_conf().servicesession.clone());
                f
            })
            .collect();
        Ok(flist)
    }

    fn get_all_files(&self) -> Result<Vec<FileInfo>, Error> {
        let flist: Vec<_> = self.gdrive.get_all_file_info(false, &self.directory_map)?;

        let flist = self.convert_gdriveinfo_to_file_info(&flist)?;

        Ok(flist)
    }

    fn get_all_changes(&self) -> Result<(Vec<String>, Vec<FileInfo>), Error> {
        let chlist: Vec<_> = self.gdrive.get_all_changes()?;
        let delete_list: Vec<_> = chlist
            .iter()
            .filter_map(|ch| match ch.file {
                Some(_) => None,
                None => ch.file_id.clone(),
            })
            .collect();
        let flist: Vec<_> = chlist.into_iter().filter_map(|ch| ch.file).collect();

        let flist = self
            .gdrive
            .convert_file_list_to_gdrive_info(&flist, &self.directory_map)?;
        let flist = self.convert_gdriveinfo_to_file_info(&flist)?;
        Ok((delete_list, flist))
    }
}

impl FileListGDriveConf {
    pub fn new(
        servicesession: &str,
        basepath: &str,
        config: &Config,
    ) -> Result<FileListGDriveConf, Error> {
        let baseurl: Url = format!("gdrive://{}/{}", servicesession, basepath).parse()?;
        let basepath = Path::new(basepath);

        let conf = FileListConf {
            baseurl,
            basepath: basepath.to_path_buf(),
            config: config.clone(),
            servicetype: FileService::GDrive,
            servicesession: servicesession.parse()?,
        };
        Ok(FileListGDriveConf(conf))
    }
}

impl FileListConfTrait for FileListGDriveConf {
    fn from_url(url: &Url, config: &Config) -> Result<FileListGDriveConf, Error> {
        if url.scheme() != "gdrive" {
            Err(err_msg("Wrong scheme"))
        } else {
            let servicesession = url
                .as_str()
                .trim_start_matches("gdrive://")
                .replace(url.path(), "");
            let tmp = format!("gdrive://{}/", servicesession);
            let basepath: Url = url.as_str().replace(&tmp, "file:///").parse()?;
            let basepath = basepath.to_file_path().map_err(|_| err_msg("Failure"))?;
            let basepath = basepath.to_string_lossy().to_string();
            let basepath = Path::new(basepath.trim_start_matches('/'));
            let conf = FileListConf {
                baseurl: url.clone(),
                basepath: basepath.to_path_buf(),
                config: config.clone(),
                servicetype: FileService::GDrive,
                servicesession: servicesession.parse()?,
            };

            Ok(FileListGDriveConf(conf))
        }
    }

    fn get_config(&self) -> &Config {
        &self.0.config
    }
}

impl FileListTrait for FileListGDrive {
    fn get_conf(&self) -> &FileListConf {
        &self.flist.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.flist.filemap
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let start_page_token = self.gdrive.get_start_page_token()?;

        let mut flist_dict = match pool {
            Some(pool) => {
                let file_list = self.load_file_list(&pool)?;
                self.get_file_list_dict(file_list, FileInfoKeyType::ServiceId)
            }
            None => HashMap::new(),
        };

        let (dlist, flist) = match self.gdrive.start_page_token {
            Some(_) => self.get_all_changes()?,
            None => {
                if let Some(pool) = pool {
                    self.clear_file_list(&pool)?;
                }
                (Vec::new(), self.get_all_files()?)
            }
        };

        debug!("delete {} insert {}", dlist.len(), flist.len());

        for dfid in &dlist {
            flist_dict.remove(dfid);
        }

        for f in flist {
            if let Some(fid) = f.serviceid.as_ref() {
                flist_dict.insert(fid.0.to_string(), f);
            }
        }

        let flist = flist_dict.into_iter().map(|(_, v)| v).collect();

        let gdrive = self.gdrive.clone().with_start_page_token(&start_page_token);
        let start_page_path = format!("{}.new", self.gdrive.start_page_token_filename);
        let start_page_path = Path::new(&start_page_path);
        gdrive.store_start_page_token(&start_page_path)?;

        Ok(flist)
    }

    fn print_list(&self) -> Result<(), Error> {
        let dnamemap = GDriveInstance::get_directory_name_map(&self.directory_map);
        let parents = if let Ok(Some(p)) =
            GDriveInstance::get_parent_id(&self.get_conf().baseurl, &dnamemap)
        {
            Some(vec![p])
        } else if let Some(root_dir) = self.root_directory.as_ref() {
            Some(vec![root_dir.clone()])
        } else {
            None
        };
        self.gdrive.process_list_of_keys(parents, |i| {
            if let Ok(finfo) = GDriveInfo::from_object(i.clone(), &self.gdrive, &self.directory_map)
                .and_then(FileInfoGDrive::from_gdriveinfo)
            {
                if let Some(url) = finfo.get_finfo().urlname.as_ref() {
                    writeln!(stdout().lock(), "{}", url.as_str())?;
                }
            }
            Ok(())
        })
    }

    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::GDrive && finfo1.servicetype == FileService::Local {
            let local_path = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| err_msg("No local path"))?;
            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| err_msg("No local path"))?
                .parent()
                .ok_or_else(|| err_msg("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(&parent_dir)?;
            }
            let gdriveid = finfo0
                .serviceid
                .clone()
                .ok_or_else(|| err_msg("No gdrive url"))?
                .0;
            let gfile = self.gdrive.get_file_metadata(&gdriveid)?;
            debug!("{:?}", gfile.mime_type);
            if GDriveInstance::is_unexportable(&gfile.mime_type) {
                debug!("unexportable");
                if let Some(pool) = self.pool.as_ref() {
                    self.remove_by_id(pool, &gdriveid)?;
                    debug!("removed from database");
                }
                return Ok(());
            }
            self.gdrive
                .download(&gdriveid, &local_path, &gfile.mime_type)
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        }
    }

    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::GDrive {
            let local_file = finfo0
                .filepath
                .clone()
                .ok_or_else(|| err_msg("No local path"))?
                .canonicalize()?;
            let local_url = Url::from_file_path(local_file).map_err(|_| err_msg("failure"))?;

            let remote_url = finfo1
                .urlname
                .clone()
                .ok_or_else(|| err_msg("No remote url"))?;
            let dnamemap = GDriveInstance::get_directory_name_map(&self.directory_map);
            let parent_id = GDriveInstance::get_parent_id(&remote_url, &dnamemap)?
                .ok_or_else(|| err_msg("No parent id!"))?;
            self.gdrive.upload(&local_url, &parent_id)?;
            Ok(())
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
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
        let gdriveid = &finfo0
            .serviceid
            .as_ref()
            .ok_or_else(|| err_msg("No serviceid"))?
            .0;
        let url = finfo1.urlname.as_ref().ok_or_else(|| err_msg("No url"))?;
        let dnamemap = GDriveInstance::get_directory_name_map(&self.directory_map);
        let parentid = GDriveInstance::get_parent_id(&url, &dnamemap)?
            .ok_or_else(|| err_msg("No parentid"))?;
        self.gdrive.move_to(gdriveid, &parentid, &finfo1.filename)
    }

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait,
    {
        let finfo = finfo.get_finfo();
        if finfo.servicetype != FileService::GDrive {
            Err(err_msg("Wrong service type"))
        } else if let Some(gdriveid) = finfo.serviceid.as_ref() {
            self.gdrive.delete_permanently(&gdriveid.0).map(|_| ())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use gdrive_lib::gdrive_instance::GDriveInstance;

    use crate::config::Config;
    use crate::file_list::FileListTrait;
    use crate::file_list_gdrive::{FileListGDrive, FileListGDriveConf};
    use crate::pgpool::PgPool;

    #[test]
    #[ignore]
    fn test_gdrive_fill_file_list() {
        let config = Config::init_config().unwrap();
        let mut gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            "ddboline@gmail.com",
        );
        gdrive.start_page_token = None;

        let fconf = FileListGDriveConf::new("ddboline@gmail.com", "My Drive", &config).unwrap();
        let flist = FileListGDrive::from_conf(fconf, &gdrive, None)
            .unwrap()
            .max_keys(100)
            .set_directory_map(false, None)
            .unwrap();

        let new_flist = flist.fill_file_list(None).unwrap();

        assert!(new_flist.len() > 0);

        let config = Config::init_config().unwrap();
        let pool = PgPool::new(&config.database_url);
        flist.clear_file_list(&pool).unwrap();

        let flist = flist.with_list(new_flist);

        println!("wrote {}", flist.cache_file_list(&pool).unwrap());

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(flist.flist.filemap.len(), new_flist.len());

        flist.clear_file_list(&pool).unwrap();

        println!("dmap {}", flist.directory_map.len());

        let dnamemap = GDriveInstance::get_directory_name_map(&flist.directory_map);
        for f in flist.get_filemap().values() {
            let u = f.urlname.as_ref().unwrap();
            let parent_id = GDriveInstance::get_parent_id(u, &dnamemap).unwrap();
            assert!(!parent_id.is_none());
            println!("{} {:?}", u, parent_id);
        }

        let multimap: HashMap<_, _> = dnamemap.iter().filter(|(_, v)| v.len() > 1).collect();
        println!("multimap {}", multimap.len());
        for (key, val) in &multimap {
            if val.len() > 1 {
                println!("{} {}", key, val.len());
            }
        }
    }
}
