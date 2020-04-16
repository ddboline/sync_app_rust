use anyhow::{format_err, Error};
use std::path::Path;
use url::Url;

use crate::{
    file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum},
    file_service::FileService,
};

#[derive(Debug, Clone)]
pub struct FileInfoSSH(pub FileInfo);

impl FileInfoSSH {
    pub fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() == "ssh" {
            let path = url.path();
            let filepath = Path::new(&path);
            let filename = filepath
                .file_name()
                .ok_or_else(|| format_err!("Parse failure"))?
                .to_os_string()
                .to_string_lossy().into_owned().into();
            let finfo = FileInfo::new(
                filename,
                Some(filepath.to_path_buf().into()),
                Some(url.clone().into()),
                None,
                None,
                None,
                None,
                FileService::SSH,
                None,
            );
            Ok(Self(finfo))
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }
}
impl FileInfoTrait for FileInfoSSH {
    fn get_finfo(&self) -> &FileInfo {
        &self.0
    }

    fn into_finfo(self) -> FileInfo {
        self.0
    }

    fn get_md5(&self) -> Option<Md5Sum> {
        self.0.md5sum.clone()
    }

    fn get_sha1(&self) -> Option<Sha1Sum> {
        self.0.sha1sum.clone()
    }

    fn get_stat(&self) -> Option<FileStat> {
        self.0.filestat
    }
}

#[cfg(test)]
mod tests {
    use crate::{file_info::FileInfoTrait, file_info_ssh::FileInfoSSH};
    use url::Url;

    #[test]
    fn test_file_info_ssh() {
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/movie_queue.sql"
            .parse()
            .unwrap();

        let finfo = FileInfoSSH::from_url(&url).unwrap();

        assert_eq!(
            finfo.get_finfo().urlname.as_ref().unwrap().as_str(),
            "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/movie_queue.sql"
        );
        assert_eq!(&finfo.get_finfo().filename, "movie_queue.sql");
    }
}
