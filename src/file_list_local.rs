use failure::{err_msg, Error};
use rayon::prelude::*;
use reqwest::Url;
use std::path::PathBuf;
use walkdir::WalkDir;

use crate::file_info::ServiceSession;
use crate::file_info_local::FileInfoLocal;
use crate::file_list::{FileList, FileListConf, FileListTrait};
use crate::file_service::FileService;

pub struct FileListLocalConf(pub FileListConf);

impl FileListLocalConf {
    pub fn new(basedir: PathBuf, session_name: String) -> Result<FileListLocalConf, Error> {
        let baseurl: Url = format!(
            "file://{}",
            basedir
                .to_str()
                .ok_or_else(|| err_msg("Failed to parse path"))?
        )
        .parse()?;
        let conf = FileListConf {
            basedir,
            baseurl,
            servicetype: FileService::Local,
            servicesession: ServiceSession(session_name),
        };
        Ok(FileListLocalConf(conf))
    }
}

impl FileListTrait for FileListLocalConf {
    fn fill_file_list(conf: FileListConf) -> Result<FileList, Error> {
        let wdir = WalkDir::new(&conf.basedir).same_file_system(true);

        let entries: Vec<_> = wdir.into_iter().filter_map(|entry| entry.ok()).collect();

        let flist = entries
            .into_par_iter()
            .filter_map(|entry| FileInfoLocal::from_direntry(entry).ok().map(|x| x.0))
            .collect();

        Ok(FileList {
            conf,
            filelist: flist,
        })
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Url;
    use std::collections::HashMap;

    use crate::file_list_local::{FileListLocalConf, FileListTrait};
    use crate::file_service::FileService;

    #[test]
    fn create_conf() {
        let basepath = "/test/path/file.txt".parse().unwrap();
        let baseurl: Url = "file:///test/path/file.txt".parse().unwrap();
        let conf = FileListLocalConf::new(basepath, "test_session".to_string());
        assert_eq!(conf.is_ok(), true);
        let conf = conf.unwrap();
        assert_eq!(conf.0.servicetype, FileService::Local);
        println!("{:?}", conf.0.baseurl);
        assert_eq!(conf.0.baseurl, baseurl);
    }

    #[test]
    fn test_fill_file_list() {
        let basepath = "src".parse().unwrap();
        let conf = FileListLocalConf::new(basepath, "test_session".to_string()).unwrap();

        let flist = FileListLocalConf::fill_file_list(conf.0).unwrap();

        for entry in &flist.filelist {
            println!("{:?}", entry);
        }

        let fset: HashMap<_, _> = flist
            .filelist
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
    }
}
