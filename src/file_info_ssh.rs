use failure::{err_msg, Error};
use std::path::Path;
use url::Url;

use crate::file_info::{FileInfo, FileInfoTrait, FileStat, Md5Sum, Sha1Sum};
use crate::file_service::FileService;

#[derive(Debug, Clone)]
pub struct FileInfoSSH(pub FileInfo);

impl FileInfoTrait for FileInfoSSH {
    fn from_url(url: &Url) -> Result<FileInfoSSH, Error> {
        if url.scheme() != "ssh" {
            Err(err_msg("Wrong scheme"))
        } else {
            let path = url.path();
            let filepath = Path::new(&path);
            let filename = filepath
                .file_name()
                .ok_or_else(|| err_msg("Parse failure"))?
                .to_os_string()
                .into_string()
                .map_err(|_| err_msg("Parse failure"))?;
            let finfo = FileInfo {
                filename,
                filepath: Some(filepath.to_path_buf()),
                urlname: Some(url.clone()),
                md5sum: None,
                sha1sum: None,
                filestat: None,
                serviceid: None,
                servicetype: FileService::SSH,
                servicesession: None,
            };
            Ok(FileInfoSSH(finfo))
        }
    }

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
    use crate::file_info::FileInfoTrait;
    use crate::file_info_ssh::FileInfoSSH;
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
        assert_eq!(
            &finfo.get_md5().unwrap().0,
            "33a52453f4f07c4d4491a41a1b3c7e5b"
        );
        assert_eq!(
            &finfo.get_sha1().unwrap().0,
            "a674b70761141d7814ebed059e8d42cfe42f7dd5"
        );
        assert_eq!(finfo.get_stat().unwrap().st_size, 128217549);
        println!("{:?}", finfo.get_finfo());
    }
}