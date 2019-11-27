use failure::{err_msg, format_err, Error};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;
use std::fs::{copy, create_dir_all, remove_file, rename};
use std::io::{stdout, Write};
use std::path::PathBuf;
use std::string::ToString;
use std::time::SystemTime;
use url::Url;
use walkdir::WalkDir;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait};
use crate::file_info_local::FileInfoLocal;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;

#[derive(Debug, Clone)]
pub struct FileListLocal(pub FileList);

#[derive(Debug, Clone)]
pub struct FileListLocalConf(pub FileListConf);

impl FileListLocal {
    pub fn from_conf(conf: FileListLocalConf) -> FileListLocal {
        FileListLocal(FileList {
            conf: conf.0,
            filemap: HashMap::new(),
        })
    }

    pub fn with_list(&self, filelist: Vec<FileInfo>) -> FileListLocal {
        FileListLocal(self.0.with_list(filelist))
    }
}

impl FileListLocalConf {
    pub fn new(basedir: PathBuf, config: &Config) -> Result<FileListLocalConf, Error> {
        let basepath = basedir.canonicalize()?;
        let basestr = basepath.to_string_lossy().to_string();
        let baseurl =
            Url::from_file_path(basepath.clone()).map_err(|_| err_msg("Failed to parse url"))?;
        let conf = FileListConf {
            baseurl,
            basepath,
            config: config.clone(),
            servicetype: FileService::Local,
            servicesession: basestr.parse()?,
        };
        Ok(FileListLocalConf(conf))
    }
}

impl FileListConfTrait for FileListLocalConf {
    fn from_url(url: &Url, config: &Config) -> Result<FileListLocalConf, Error> {
        if url.scheme() != "file" {
            Err(err_msg("Wrong scheme"))
        } else {
            let path = url.to_file_path().map_err(|_| err_msg("Parse failure"))?;
            let basestr = path.to_string_lossy().to_string();
            let conf = FileListConf {
                baseurl: url.clone(),
                basepath: path,
                config: config.clone(),
                servicetype: FileService::Local,
                servicesession: basestr.parse()?,
            };
            Ok(FileListLocalConf(conf))
        }
    }

    fn get_config(&self) -> &Config {
        &self.0.config
    }
}

impl FileListTrait for FileListLocal {
    fn get_conf(&self) -> &FileListConf {
        &self.0.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.0.filemap
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();
        let basedir = conf.baseurl.path();
        let flist_dict = match pool {
            Some(pool) => {
                let file_list = self.load_file_list(&pool)?;
                self.get_file_list_dict(file_list, FileInfoKeyType::FilePath)
            }
            None => HashMap::new(),
        };

        let wdir = WalkDir::new(&basedir).same_file_system(true);

        let entries: Vec<_> = wdir.into_iter().filter_map(Result::ok).collect();

        let flist = entries
            .into_par_iter()
            .filter_map(|entry| {
                let filepath = entry
                    .path()
                    .canonicalize()
                    .ok()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "".to_string());
                let (modified, size) = entry
                    .metadata()
                    .map(|metadata| {
                        let modified = metadata
                            .modified()
                            .unwrap()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as u32;
                        let size = metadata.len() as u32;
                        (modified, size)
                    })
                    .unwrap_or_else(|_| (0, 0));
                if let Some(finfo) = flist_dict.get(&filepath) {
                    if let Some(fstat) = finfo.filestat {
                        if fstat.st_mtime >= modified && fstat.st_size == size {
                            return Some(finfo.clone());
                        }
                    }
                };
                FileInfoLocal::from_direntry(
                    entry,
                    Some(conf.servicesession.0.to_string().into()),
                    Some(conf.servicesession.clone()),
                )
                .ok()
                .map(|x| x.0)
            })
            .collect();

        Ok(flist)
    }

    fn print_list(&self) -> Result<(), Error> {
        let conf = self.get_conf();
        let basedir = conf.baseurl.path();

        let wdir = WalkDir::new(&basedir).same_file_system(true).max_depth(1);

        let entries: Vec<_> = wdir.into_iter().filter_map(Result::ok).collect();

        entries
            .into_par_iter()
            .map(|entry| {
                let filepath = entry
                    .path()
                    .canonicalize()
                    .ok()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "".to_string());
                writeln!(stdout().lock(), "{}", filepath)?;
                Ok(())
            })
            .collect()
    }

    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype != FileService::Local || finfo1.servicetype != FileService::Local {
            Err(format_err!(
                "Wrong fileinfo types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        } else {
            let local_file = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| err_msg("No local path"))?;
            let remote_file = finfo0
                .filepath
                .clone()
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

            copy(&remote_file, &local_file)?;
            Ok(())
        }
    }

    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        self.copy_from(finfo0, finfo1)
    }

    fn move_file<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        let path0 = finfo0
            .filepath
            .as_ref()
            .ok_or_else(|| err_msg("No file path"))?;
        let path1 = finfo1
            .filepath
            .as_ref()
            .ok_or_else(|| err_msg("No file path"))?;
        if finfo0.servicetype != FileService::Local || finfo1.servicetype != FileService::Local {
            Ok(())
        } else {
            rename(&path0, path1).or_else(|_| {
                copy(&path0, &path1)?;
                remove_file(&path0)?;
                Ok(())
            })
        }
    }

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait,
    {
        let finfo = finfo.get_finfo();
        if finfo.servicetype != FileService::Local {
            Err(err_msg("Wrong service type"))
        } else if let Some(filepath) = finfo.filepath.as_ref() {
            remove_file(filepath).map_err(err_msg)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use url::Url;

    use crate::config::Config;
    use crate::file_info::FileInfo;
    use crate::file_list::FileList;
    use crate::file_list_local::{FileListLocal, FileListLocalConf, FileListTrait};
    use crate::file_service::FileService;
    use crate::models::{FileInfoCache, InsertFileInfoCache};
    use crate::pgpool::PgPool;

    #[test]
    fn create_conf() {
        let basepath: PathBuf = "src".parse().unwrap();
        let baseurl: Url = format!(
            "file://{}",
            basepath.canonicalize().unwrap().to_string_lossy()
        )
        .parse()
        .unwrap();
        let config = Config::init_config().unwrap();
        let conf = FileListLocalConf::new(basepath, &config);
        println!("{:?}", conf);
        assert_eq!(conf.is_ok(), true);
        let conf = conf.unwrap();
        assert_eq!(conf.0.servicetype, FileService::Local);
        println!("{:?}", conf.0.baseurl);
        assert_eq!(conf.0.baseurl, baseurl);
    }

    #[test] #[ignore]
    fn test_fill_file_list() {
        let basepath = "src".parse().unwrap();
        let config = Config::init_config().unwrap();
        let conf = FileListLocalConf::new(basepath, &config).unwrap();
        let flist = FileListLocal(FileList {
            conf: conf.0.clone(),
            filemap: HashMap::new(),
        });

        let new_flist = flist.fill_file_list(None).unwrap();

        println!("0 {}", new_flist.len());

        let fset: HashMap<_, _> = new_flist
            .iter()
            .map(|f| (f.filename.clone(), f.clone()))
            .collect();

        assert_eq!(fset.contains_key("file_list_local.rs"), true);

        let result = fset.get("file_list_local.rs").unwrap();

        println!("{:?}", result);

        assert!(result
            .filepath
            .as_ref()
            .unwrap()
            .ends_with("file_list_local.rs"));
        assert!(result
            .urlname
            .as_ref()
            .unwrap()
            .as_str()
            .ends_with("file_list_local.rs"));

        let cache_info: InsertFileInfoCache = result.into();
        println!("{:?}", cache_info);
        assert_eq!(
            &result.md5sum.as_ref().unwrap().0,
            cache_info.md5sum.as_ref().unwrap()
        );

        let cache_info = FileInfoCache::from_insert(cache_info, 5);
        let test_result = FileInfo::from_cache_info(&cache_info).unwrap();
        assert_eq!(*result, test_result);

        let config = Config::init_config().unwrap();
        let pool = PgPool::new(&config.database_url);

        println!("1 {}", new_flist.len());

        let flist = FileListLocal::from_conf(conf).with_list(new_flist);

        println!("2 {}", flist.get_filemap().len());

        println!("wrote {}", flist.cache_file_list(&pool).unwrap());

        println!("{:?}", flist.get_conf().servicesession);

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(new_flist.len(), flist.0.filemap.len());

        println!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        let new_flist = flist.fill_file_list(Some(&pool)).unwrap();

        assert_eq!(new_flist.len(), flist.0.filemap.len());

        println!("{}", new_flist.len());
        assert!(new_flist.len() != 0);

        flist.clear_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(new_flist.len(), 0);
    }
}
