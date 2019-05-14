use failure::{err_msg, Error};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::create_dir_all;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_info_gdrive::FileInfoGDrive;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::gdrive_instance::GDriveInstance;
use crate::map_result_vec;
use crate::pgpool::PgPool;

#[derive(Debug, Clone)]
pub struct FileListGDrive {
    pub flist: FileList,
    pub gdrive: GDriveInstance,
}

impl FileListGDrive {
    pub fn from_conf(conf: FileListGDriveConf, gdrive: &GDriveInstance) -> FileListGDrive {
        FileListGDrive {
            flist: FileList::from_conf(conf.0),
            gdrive: gdrive.clone(),
        }
    }

    pub fn with_list(&self, filelist: &[FileInfo]) -> FileListGDrive {
        FileListGDrive {
            flist: self.flist.with_list(&filelist),
            gdrive: self.gdrive.clone(),
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.gdrive = self.gdrive.with_max_keys(max_keys);
        self
    }
}

#[derive(Debug, Clone)]
pub struct FileListGDriveConf(pub FileListConf);

impl FileListGDriveConf {
    pub fn new(basepath: &str, config: &Config) -> Result<FileListGDriveConf, Error> {
        let baseurl: Url = format!("gdrive://{}", basepath).parse()?;

        let conf = FileListConf {
            baseurl,
            config: config.clone(),
            servicetype: FileService::GDrive,
            servicesession: basepath.parse()?,
        };
        Ok(FileListGDriveConf(conf))
    }
}

impl FileListConfTrait for FileListGDriveConf {
    fn from_url(url: &Url, config: &Config) -> Result<FileListGDriveConf, Error> {
        if url.scheme() != "gdrive" {
            Err(err_msg("Wrong scheme"))
        } else {
            let bucket = url.host_str().ok_or_else(|| err_msg("Parse error"))?;
            let conf = FileListConf {
                baseurl: url.clone(),
                config: config.clone(),
                servicetype: FileService::GDrive,
                servicesession: bucket.parse()?,
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

    fn fill_file_list(&self, _: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let flist: Vec<_> = self.gdrive.get_all_files(None)?;

        let flist: Vec<Result<_, Error>> = flist
            .into_par_iter()
            .map(|f| FileInfoGDrive::from_object(f).map(|i| i.0))
            .collect();
        let flist = map_result_vec(flist)?;

        Ok(flist)
    }

    fn print_list(&self) -> Result<(), Error> {
        self.gdrive.process_list_of_keys(None, |i| {
            println!(
                "gdrive://{}",
                i.name.as_ref().map(String::as_str).unwrap_or_else(|| "")
            );
        })
    }

    fn upload_file<T, U>(&self, finfo_local: &T, finfo_remote: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_local = finfo_local.get_finfo();
        let finfo_remote = finfo_remote.get_finfo();
        if finfo_local.servicetype != FileService::Local
            || finfo_remote.servicetype != FileService::GDrive
        {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        let local_file = finfo_local
            .filepath
            .clone()
            .ok_or_else(|| err_msg("No local path"))?
            .canonicalize()?;
        let local_url = Url::from_file_path(local_file).map_err(|_| err_msg("failure"))?;
        let remote_url = finfo_remote
            .urlname
            .clone()
            .ok_or_else(|| err_msg("No gdrive url"))?;
        self.gdrive.upload(&local_url, &remote_url)
    }

    fn download_file<T, U>(&self, finfo_remote: &T, finfo_local: &U) -> Result<(), Error>
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo_remote = finfo_remote.get_finfo();
        let finfo_local = finfo_local.get_finfo();
        if finfo_local.servicetype != FileService::Local
            || finfo_remote.servicetype != FileService::GDrive
        {
            return Err(err_msg(format!(
                "Wrong fileinfo types {} {}",
                finfo_local.servicetype, finfo_remote.servicetype
            )));
        }
        let local_file = finfo_local
            .filepath
            .clone()
            .ok_or_else(|| err_msg("No local path"))?
            .to_str()
            .ok_or_else(|| err_msg("Failed to parse path"))?
            .to_string();
        let local_url = Url::from_file_path(local_file).map_err(|_| err_msg("failure"))?;
        let parent_dir = finfo_local
            .filepath
            .as_ref()
            .ok_or_else(|| err_msg("No local path"))?
            .parent()
            .ok_or_else(|| err_msg("No parent directory"))?;
        if !parent_dir.exists() {
            create_dir_all(&parent_dir)?;
        }
        let gdriveid = finfo_remote
            .serviceid
            .clone()
            .ok_or_else(|| err_msg("No gdrive url"))?
            .0;
        let md5sum = self.gdrive.download(&gdriveid, &local_url, None)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::file_list::FileListTrait;
    use crate::file_list_gdrive::{FileListGDrive, FileListGDriveConf};
    use crate::gdrive_instance::GDriveInstance;
    use crate::pgpool::PgPool;

    #[test]
    fn test_fill_file_list() {
        let config = Config::new();
        let gdrive = GDriveInstance::new(&config)
            .with_max_keys(100)
            .with_page_size(100);

        let blist = gdrive.get_list_of_buckets().unwrap();
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());

        let conf = FileListGDriveConf::new("test", &config).unwrap();
        let flist = FileListGDrive::from_conf(conf).max_keys(100);

        let new_flist = flist.fill_file_list(None).unwrap();

        println!("{} {:?}", bucket, new_flist.get(0));
        assert!(new_flist.len() > 0);

        let config = Config::new();
        let pool = PgPool::new(&config.database_url);
        let flist = flist.with_list(&new_flist);

        flist.cache_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(flist.flist.filemap.len(), new_flist.len());

        flist.clear_file_list(&pool).unwrap();
    }
}
