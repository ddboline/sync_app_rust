use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::collections::HashMap;
use std::path::PathBuf;
use std::string::ToString;
use std::time::SystemTime;
use walkdir::WalkDir;

use crate::file_info::FileInfo;
use crate::file_info_local::FileInfoLocal;
use crate::file_list::{FileList, FileListConf, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;

#[derive(Debug)]
pub struct FileListLocal(pub FileList);

#[derive(Debug)]
pub struct FileListLocalConf(pub FileListConf);

impl FileListLocalConf {
    pub fn new(basedir: PathBuf) -> Result<FileListLocalConf, Error> {
        let basepath = basedir.canonicalize()?;
        let basestr = basepath
            .to_str()
            .ok_or_else(|| err_msg("Failed to parse path"))?
            .to_string();
        let baseurl =
            Url::from_file_path(basepath.clone()).map_err(|_| err_msg("Failed to parse url"))?;
        let conf = FileListConf {
            baseurl,
            servicetype: FileService::Local,
            servicesession: basestr.parse()?,
            serviceid: basestr.into(),
        };
        Ok(FileListLocalConf(conf))
    }
}

impl FileListTrait for FileListLocal {
    fn get_conf(&self) -> &FileListConf {
        &self.0.conf
    }

    fn get_filelist(&self) -> &[FileInfo] {
        &self.0.filelist
    }

    fn fill_file_list(&self, pool: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();
        let basedir = conf.baseurl.path();
        let flist_dict = match pool {
            Some(pool) => self.get_file_list_dict(&pool)?,
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
                    .and_then(|s| s.to_str().map(ToString::to_string))
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
                    Some(conf.serviceid.clone()),
                    Some(conf.servicesession.clone()),
                )
                .ok()
                .map(|x| x.0)
            })
            .collect();

        Ok(flist)
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Url;
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::config::Config;
    use crate::file_info::FileInfo;
    use crate::file_list::FileList;
    use crate::file_list_local::{FileListLocal, FileListLocalConf, FileListTrait};
    use crate::file_service::FileService;
    use crate::models::FileInfoCache;
    use crate::pgpool::PgPool;

    #[test]
    fn create_conf() {
        let basepath: PathBuf = "src".parse().unwrap();
        let baseurl: Url = format!(
            "file://{}",
            basepath.canonicalize().unwrap().to_str().unwrap()
        )
        .parse()
        .unwrap();
        let conf = FileListLocalConf::new(basepath);
        println!("{:?}", conf);
        assert_eq!(conf.is_ok(), true);
        let conf = conf.unwrap();
        assert_eq!(conf.0.servicetype, FileService::Local);
        println!("{:?}", conf.0.baseurl);
        assert_eq!(conf.0.baseurl, baseurl);
    }

    #[test]
    fn test_fill_file_list() {
        let basepath = "src".parse().unwrap();
        let conf = FileListLocalConf::new(basepath).unwrap();
        let flist = FileListLocal(FileList {
            conf: conf.0.clone(),
            filelist: Vec::new(),
        });

        let flist = flist.fill_file_list(None).unwrap();

        for entry in &flist {
            println!("{:?}", entry);
        }

        let fset: HashMap<_, _> = flist
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

        let cache_info = result.get_cache_info().unwrap();
        println!("{:?}", cache_info);
        assert_eq!(
            result.md5sum.clone().unwrap().0,
            cache_info.md5sum.clone().unwrap()
        );

        let cache_info = FileInfoCache::from_insert(cache_info, 5);
        let test_result = FileInfo::from_cache_info(cache_info).unwrap();
        assert_eq!(*result, test_result);

        let config = Config::new();
        let pool = PgPool::new(&config.database_url);

        let flist = FileListLocal(FileList {
            conf: conf.0,
            filelist: flist,
        });

        flist.cache_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(new_flist.len(), flist.0.filelist.len());

        assert!(new_flist.len() != 0);

        let new_flist = flist.fill_file_list(Some(&pool)).unwrap();

        assert_eq!(new_flist.len(), flist.0.filelist.len());

        assert!(new_flist.len() != 0);

        flist.clear_file_list(&pool).unwrap();

        let new_flist = flist.load_file_list(&pool).unwrap();

        assert_eq!(new_flist.len(), 0);
    }
}
