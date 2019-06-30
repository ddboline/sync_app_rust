use failure::{err_msg, Error};
use std::collections::HashMap;
use std::path::Path;
use subprocess::Exec;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoKeyType, FileInfoTrait};
use crate::file_info_ssh::FileInfoSSH;
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;

pub struct FileListSSH(pub FileList);

#[derive(Debug)]
pub struct FileListSSHConf(pub FileListConf);

impl FileListConfTrait for FileListSSHConf {
    fn from_url(url: &Url, config: &Config) -> Result<Self, Error> {
        if url.scheme() != "ssh" {
            Err(err_msg("Wrong scheme"))
        } else {
            let basepath = Path::new(url.path());
            let host = url.host_str().ok_or_else(|| err_msg("Parse error"))?;
            let port = url.port().unwrap_or(22);
            let username = url.username().to_string();
            let session = format!(
                "ssh://{}@{}:{}{}",
                username,
                host,
                port,
                basepath.to_str().unwrap_or("")
            );
            let conf = FileListConf {
                baseurl: url.clone(),
                basepath: basepath.to_path_buf(),
                config: config.clone(),
                servicetype: FileService::SSH,
                servicesession: session.parse()?,
            };

            Ok(FileListSSHConf(conf))
        }
    }

    fn get_config(&self) -> &Config {
        self.0.get_config()
    }
}

impl FileListTrait for FileListSSH {
    fn get_conf(&self) -> &FileListConf {
        &self.0.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.0.filemap
    }

    // Copy operation where the origin (finfo0) has the same servicetype as self
    fn copy_from<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::SSH && finfo1.servicetype == FileService::Local {
            let url0 = finfo0
                .get_finfo()
                .urlname
                .as_ref()
                .ok_or_else(|| err_msg("No url"))?;
            let command = format!(
                "scp {} {}",
                FileInfoSSH::get_ssh_str(&url0)?,
                finfo1
                    .filepath
                    .as_ref()
                    .ok_or_else(|| err_msg("No path"))?
                    .to_str()
                    .ok_or_else(|| err_msg("Invalid String"))?
            );
            println!("command {}", command);
            let status = Exec::shell(command).join()?;
            if !status.success() {
                Err(err_msg("Scp failed"))
            } else {
                Ok(())
            }
        } else {
            Err(err_msg(format!(
                "Invalid types {} {}",
                finfo0.servicetype, finfo1.servicetype
            )))
        }
    }

    // Copy operation where the destination (finfo0) has the same servicetype as self
    fn copy_to<T, U>(&self, finfo0: &T, finfo1: &U) -> Result<(), Error>
    where
        T: FileInfoTrait,
        U: FileInfoTrait,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::SSH {
            let url1 = finfo1
                .get_finfo()
                .urlname
                .as_ref()
                .ok_or_else(|| err_msg("No url"))?;
            let command = format!(
                "scp {} {}",
                finfo0
                    .filepath
                    .as_ref()
                    .ok_or_else(|| err_msg("No path"))?
                    .to_str()
                    .ok_or_else(|| err_msg("Invalid String"))?,
                FileInfoSSH::get_ssh_str(&url1)?,
            );
            println!("command {}", command);
            let status = Exec::shell(command).join()?;
            if !status.success() {
                Err(err_msg("Scp failed"))
            } else {
                Ok(())
            }
        } else {
            Err(err_msg(format!(
                "Invalid types {} {}",
                finfo0.servicetype, finfo1.servicetype
            )))
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
        let url0 = finfo0
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| err_msg("No url"))?;
        let url1 = finfo1
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| err_msg("No url"))?;

        let user_host0 = FileInfoSSH::get_ssh_username_host(&url0)?;
        let user_host1 = FileInfoSSH::get_ssh_username_host(&url1)?;
        if user_host0 != user_host1 {
            return Ok(());
        }
        let url0 = finfo0
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| err_msg("No url"))?;
        let path0 = Path::new(url0.path())
            .to_str()
            .ok_or_else(|| err_msg("Invalid path"))?;
        let url1 = finfo1
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| err_msg("No url"))?;
        let path1 = Path::new(url1.path())
            .to_str()
            .ok_or_else(|| err_msg("Invalid path"))?;
        let command = format!(r#"ssh {} "{} {}""#, user_host0, path0, path1);
        println!("command {}", command);
        let status = Exec::shell(command).join()?;
        if !status.success() {
            Err(err_msg("Scp failed"))
        } else {
            Ok(())
        }
    }

    fn delete<T>(&self, finfo: &T) -> Result<(), Error>
    where
        T: FileInfoTrait,
    {
        let finfo = finfo.get_finfo();
        let url = finfo
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| err_msg("No url"))?;
        let path = Path::new(url.path())
            .to_str()
            .ok_or_else(|| err_msg("Invalid path"))?;
        let user_host = FileInfoSSH::get_ssh_username_host(&url)?;
        let command = format!(r#"ssh {} "rm {}""#, user_host, path);
        println!("command {}", command);
        let status = Exec::shell(command).join()?;
        if !status.success() {
            Err(err_msg("Scp failed"))
        } else {
            Ok(())
        }
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

        Ok(Vec::new())
    }

    fn print_list(&self) -> Result<(), Error> {
        let url = &self.get_conf().baseurl;
        let path = self
            .get_conf()
            .basepath
            .to_str()
            .ok_or_else(|| err_msg("Invalid path"))?;
        let user_host = FileInfoSSH::get_ssh_username_host(&url)?;
        let command = format!(r#"ssh {} "sync-app-rust index -u file://{}""#, user_host, path);
        println!("command {}", command);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
    use std::path::{Path, PathBuf};
    use url::Url;

    use crate::config::Config;
    use crate::file_info::FileInfoTrait;
    use crate::file_info_local::FileInfoLocal;
    use crate::file_info_ssh::FileInfoSSH;
    use crate::file_list::{FileList, FileListConfTrait, FileListTrait};
    use crate::file_list_ssh::{FileListSSH, FileListSSHConf};

    #[test]
    fn test_file_list_ssh_conf_from_url() {
        let config = Config::init_config().unwrap();
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/"
            .parse()
            .unwrap();
        let conf = FileListSSHConf::from_url(&url, &config).unwrap();
        println!("{:?}", conf);
        assert!(false);
    }

    #[test]
    fn test_file_list_ssh_copy_from() {
        let config = Config::init_config().unwrap();
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/temp0.txt"
            .parse()
            .unwrap();
        let finfo0 = FileInfoSSH::from_url(&url).unwrap();
        let url: Url = "file:///tmp/temp0.txt".parse().unwrap();
        let finfo1 = FileInfoLocal::from_url(&url).unwrap();

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/"
            .parse()
            .unwrap();
        let conf = FileListSSHConf::from_url(&url, &config).unwrap();
        let flist = FileList::from_conf(conf.0);
        let flist = FileListSSH(flist);
        flist.copy_from(&finfo0, &finfo1).unwrap();
        let p = Path::new("/tmp/temp0.txt");
        if p.exists() {
            remove_file(p).unwrap();
        }
    }

    #[test]
    fn test_file_list_ssh_copy_to() {
        let config = Config::init_config().unwrap();

        let path: PathBuf = "src/file_list_ssh.rs".parse().unwrap();
        let url: Url = format!("file://{}", path.canonicalize().unwrap().to_str().unwrap())
            .parse()
            .unwrap();
        let finfo0 = FileInfoLocal::from_url(&url).unwrap();
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/file_list_ssh.rs"
            .parse()
            .unwrap();
        let finfo1 = FileInfoSSH::from_url(&url).unwrap();

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/".parse().unwrap();
        let conf = FileListSSHConf::from_url(&url, &config).unwrap();
        let flist = FileList::from_conf(conf.0);
        let flist = FileListSSH(flist);

        flist.copy_to(&finfo0, &finfo1).unwrap();
        flist.delete(&finfo1).unwrap();
    }

}
