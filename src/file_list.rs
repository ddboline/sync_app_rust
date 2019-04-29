use failure::Error;
use reqwest::Url;
use std::path::PathBuf;

use crate::file_info::{FileInfo, ServiceSession};
use crate::file_service::FileService;

pub struct FileListConf {
    pub basedir: PathBuf,
    pub baseurl: Url,
    pub servicetype: FileService,
    pub servicesession: ServiceSession,
}

pub struct FileList {
    pub conf: FileListConf,
    pub filelist: Vec<FileInfo>,
}

pub trait FileListTrait {
    fn fill_file_list(conf: FileListConf) -> Result<FileList, Error>;
}
