use anyhow::{format_err, Error};
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
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait, ServiceSession};
use crate::file_info_gdrive::FileInfoGDrive;
use crate::file_list::{FileList, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;

#[derive(Debug, Clone)]
pub struct FileListGDrive {
    pub flist: FileList,
    pub gdrive: GDriveInstance,
    pub directory_map: Arc<HashMap<String, DirectoryInfo>>,
    pub root_directory: Option<String>,
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

        let flist = FileList {
            baseurl,
            basepath: basepath.to_path_buf(),
            config: config.clone(),
            servicetype: FileService::GDrive,
            servicesession: servicesession.parse()?,
            pool: pool.clone(),
            filemap: HashMap::new(),
        };

        let gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            &flist.servicesession.0,
        );

        Ok(Self {
            flist,
            gdrive,
            directory_map: Arc::new(HashMap::new()),
            root_directory: None,
        })
    }

    pub fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "gdrive" {
            let servicesession = url
                .as_str()
                .trim_start_matches("gdrive://")
                .replace(url.path(), "");
            let tmp = format!("gdrive://{}/", servicesession);
            let basepath: Url = url.as_str().replace(&tmp, "file:///").parse()?;
            let basepath = basepath
                .to_file_path()
                .map_err(|_| format_err!("Failure"))?;
            let basepath = basepath.to_string_lossy().to_string();
            let basepath = Path::new(basepath.trim_start_matches('/'));
            let flist = FileList {
                baseurl: url.clone(),
                basepath: basepath.to_path_buf(),
                config: config.clone(),
                servicetype: FileService::GDrive,
                servicesession: servicesession.parse()?,
                pool: pool.clone(),
                filemap: HashMap::new(),
            };

            let gdrive = GDriveInstance::new(
                &config.gdrive_token_path,
                &config.gdrive_secret_file,
                &flist.servicesession.0,
            );

            Ok(Self {
                flist,
                gdrive,
                directory_map: Arc::new(HashMap::new()),
                root_directory: None,
            })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }

    pub fn set_directory_map(mut self, use_cache: bool) -> Result<Self, Error> {
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
        self.directory_map = Arc::new(dmap);
        self.root_directory = root_dir;

        Ok(self)
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
                    if url.as_str().contains(self.get_baseurl().as_str()) {
                        return true;
                    }
                }
                false
            })
            .map(|mut f| {
                f.servicesession.replace(self.get_servicesession().clone());
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

impl FileListTrait for FileListGDrive {
    fn get_baseurl(&self) -> &Url {
        &self.flist.baseurl
    }
    fn set_baseurl(&mut self, baseurl: Url) {
        self.flist.baseurl = baseurl;
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

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.flist.filemap
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.flist.with_list(filelist)
    }

    fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let start_page_token = self.gdrive.get_start_page_token()?;
        let file_list = self.load_file_list()?;
        let mut flist_dict = { self.get_file_list_dict(file_list, FileInfoKeyType::ServiceId) };

        let (dlist, flist) = if self.gdrive.start_page_token.is_some() {
            self.get_all_changes()?
        } else {
            {
                self.clear_file_list()?;
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
        let parents =
            if let Ok(Some(p)) = GDriveInstance::get_parent_id(&self.get_baseurl(), &dnamemap) {
                Some(vec![p])
            } else if let Some(root_dir) = self.root_directory.as_ref() {
                Some(vec![root_dir.clone()])
            } else {
                None
            };
        self.gdrive.process_list_of_keys(&parents, |i| {
            if let Ok(finfo) = GDriveInfo::from_object(i, &self.gdrive, &self.directory_map)
                .and_then(FileInfoGDrive::from_gdriveinfo)
            {
                if let Some(url) = finfo.get_finfo().urlname.as_ref() {
                    writeln!(stdout().lock(), "{}", url.as_str())?;
                }
            }
            Ok(())
        })
    }

    fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::GDrive && finfo1.servicetype == FileService::Local {
            let local_path = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?;
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
            let gfile = self.gdrive.get_file_metadata(&gdriveid)?;
            debug!("{:?}", gfile.mime_type);
            if GDriveInstance::is_unexportable(&gfile.mime_type) {
                debug!("unexportable");
                self.remove_by_id(&gdriveid)?;
                debug!("removed from database");
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

    fn copy_to(&self, finfo0: &dyn FileInfoTrait, finfo1: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::GDrive {
            let local_file = finfo0
                .filepath
                .clone()
                .ok_or_else(|| format_err!("No local path"))?
                .canonicalize()?;
            let local_url = Url::from_file_path(local_file).map_err(|_| format_err!("failure"))?;

            let remote_url = finfo1
                .urlname
                .clone()
                .ok_or_else(|| format_err!("No remote url"))?;
            let dnamemap = GDriveInstance::get_directory_name_map(&self.directory_map);
            let parent_id = GDriveInstance::get_parent_id(&remote_url, &dnamemap)?
                .ok_or_else(|| format_err!("No parent id!"))?;
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

    fn move_file(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype != finfo1.servicetype || self.get_servicetype() != finfo0.servicetype
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
        let dnamemap = GDriveInstance::get_directory_name_map(&self.directory_map);
        let parentid = GDriveInstance::get_parent_id(&url, &dnamemap)?
            .ok_or_else(|| format_err!("No parentid"))?;
        self.gdrive.move_to(gdriveid, &parentid, &finfo1.filename)
    }

    fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        if finfo.servicetype != FileService::GDrive {
            Err(format_err!("Wrong service type"))
        } else if let Some(gdriveid) = finfo.serviceid.as_ref() {
            self.gdrive.delete_permanently(&gdriveid.0).map(|_| ())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use std::collections::HashMap;
    use std::fs::remove_file;
    use std::io::{stdout, Write};

    use gdrive_lib::gdrive_instance::GDriveInstance;

    use crate::config::Config;
    use crate::file_list::FileListTrait;
    use crate::file_list_gdrive::{FileListGDrive, FileListGDriveConf};
    use crate::pgpool::PgPool;

    #[test]
    #[ignore]
    fn test_gdrive_fill_file_list() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let mut gdrive = GDriveInstance::new(
            &config.gdrive_token_path,
            &config.gdrive_secret_file,
            "ddboline@gmail.com",
        );
        gdrive.start_page_token = None;

        let fconf = FileListGDriveConf::new("ddboline@gmail.com", "My Drive", &config)?;
        let mut flist = FileListGDrive::from_conf(fconf, gdrive, pool.clone())?
            .max_keys(100)
            .set_directory_map(false)?;

        let new_flist = flist.fill_file_list()?;

        assert!(new_flist.len() > 0);

        flist.clear_file_list()?;

        flist.with_list(new_flist);

        writeln!(stdout(), "wrote {}", flist.cache_file_list()?)?;

        let new_flist = flist.load_file_list()?;

        assert_eq!(flist.flist.filemap.len(), new_flist.len());

        flist.clear_file_list()?;

        writeln!(stdout(), "dmap {}", flist.directory_map.len())?;

        let dnamemap = GDriveInstance::get_directory_name_map(&flist.directory_map);
        for f in flist.get_filemap().values() {
            let u = f.urlname.as_ref().unwrap();
            let parent_id = GDriveInstance::get_parent_id(u, &dnamemap)?;
            assert!(!parent_id.is_none());
            writeln!(stdout(), "{} {:?}", u, parent_id)?;
        }

        let multimap: HashMap<_, _> = dnamemap.iter().filter(|(_, v)| v.len() > 1).collect();
        writeln!(stdout(), "multimap {}", multimap.len())?;
        for (key, val) in &multimap {
            if val.len() > 1 {
                writeln!(stdout(), "{} {}", key, val.len())?;
            }
        }

        let fname = format!(
            "{}/{}_start_page_token",
            config.gdrive_token_path,
            flist.get_servicesession().0
        );
        remove_file(&fname)?;
        Ok(())
    }
}
