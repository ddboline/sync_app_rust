use anyhow::{format_err, Error};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::create_dir_all;
use std::io::{stdout, Write};
use std::path::Path;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoSerialize, FileInfoTrait};
use crate::file_list::{FileList, FileListConf, FileListConfTrait, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;
use crate::ssh_instance::SSHInstance;

#[derive(Clone, Debug)]
pub struct FileListSSH {
    pub flist: FileList,
    pub ssh: SSHInstance,
}

impl FileListSSH {
    pub fn from_conf(conf: FileListSSHConf) -> Result<Self, Error> {
        let ssh = SSHInstance::from_url(&conf.0.baseurl)?;
        Ok(Self {
            flist: FileList::from_conf(conf.0),
            ssh,
        })
    }
}

#[derive(Debug)]
pub struct FileListSSHConf(pub FileListConf);

impl FileListConfTrait for FileListSSHConf {
    fn from_url(url: &Url, config: &Config) -> Result<Self, Error> {
        if url.scheme() == "ssh" {
            let basepath = Path::new(url.path());
            let host = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
            let port = url.port().unwrap_or(22);
            let username = url.username().to_string();
            let session = format!(
                "ssh://{}@{}:{}{}",
                username,
                host,
                port,
                basepath.to_string_lossy()
            );
            let conf = FileListConf {
                baseurl: url.clone(),
                basepath: basepath.to_path_buf(),
                config: config.clone(),
                servicetype: FileService::SSH,
                servicesession: session.parse()?,
            };

            Ok(Self(conf))
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }

    fn get_config(&self) -> &Config {
        self.0.get_config()
    }
}

impl FileListTrait for FileListSSH {
    fn get_conf(&self) -> &FileListConf {
        &self.flist.conf
    }

    fn get_filemap(&self) -> &HashMap<String, FileInfo> {
        &self.flist.filemap
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
                .ok_or_else(|| format_err!("No url"))?;
            let path0 = Path::new(url0.path()).to_string_lossy();

            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(&parent_dir)?;
            }

            let command = format!(
                "scp {} {}",
                self.ssh.get_ssh_str(&path0)?,
                finfo1
                    .filepath
                    .as_ref()
                    .ok_or_else(|| format_err!("No path"))?
                    .to_string_lossy()
            );
            debug!("command {}", command);
            self.ssh.run_command(&command)
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
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
                .ok_or_else(|| format_err!("No url"))?;
            let path1 = Path::new(url1.path()).to_string_lossy();

            let parent_dir = finfo1
                .filepath
                .as_ref()
                .ok_or_else(|| format_err!("No local path"))?
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?
                .to_string_lossy();

            let command = format!("mkdir -p {}", parent_dir);
            self.ssh.run_command_ssh(&command)?;

            let command = format!(
                "scp {} {}",
                finfo0
                    .filepath
                    .as_ref()
                    .ok_or_else(|| format_err!("No path"))?
                    .to_string_lossy(),
                self.ssh.get_ssh_str(&path1)?,
            );
            debug!("command {}", command);
            self.ssh.run_command(&command)
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
        let url0 = finfo0
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| format_err!("No url"))?;
        let url1 = finfo1
            .get_finfo()
            .urlname
            .as_ref()
            .ok_or_else(|| format_err!("No url"))?;

        if url0.username() != url1.username() || url0.host_str() != url1.host_str() {
            return Ok(());
        }

        let path0 = Path::new(url0.path()).to_string_lossy();
        let path1 = Path::new(url1.path()).to_string_lossy();
        let command = format!("mv {} {}", path0, path1);
        debug!("command {}", command);
        self.ssh.run_command_ssh(&command)
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
            .ok_or_else(|| format_err!("No url"))?;
        let path = Path::new(url.path()).to_string_lossy();
        let command = format!("rm {}", path);
        self.ssh.run_command_ssh(&command)
    }

    fn fill_file_list(&self, _: Option<&PgPool>) -> Result<Vec<FileInfo>, Error> {
        let conf = self.get_conf();

        let path = conf.basepath.to_string_lossy();
        let user_host = self.ssh.get_ssh_username_host()?;
        let command = format!("sync-app-rust index -u file://{}", path);
        self.ssh.run_command_ssh(&command)?;

        let command = format!("sync-app-rust ser -u file://{}", path);

        let url_prefix = format!("ssh://{}", user_host);

        self.ssh
            .run_command_stream_stdout(&command)?
            .into_iter()
            .map(|l| {
                let finfo: FileInfoSerialize = serde_json::from_str(&l)?;
                let mut finfo: FileInfo = finfo.try_into()?;
                finfo.servicetype = FileService::SSH;
                finfo.urlname = finfo
                    .urlname
                    .and_then(|u| u.as_str().replace("file://", &url_prefix).parse().ok());
                finfo.serviceid = Some(conf.baseurl.clone().into_string().into());
                finfo.servicesession = conf.baseurl.as_str().parse().ok();
                Ok(finfo)
            })
            .collect()
    }

    fn print_list(&self) -> Result<(), Error> {
        let path = self.get_conf().basepath.to_string_lossy();
        let command = format!("sync-app-rust ls -u file://{}", path);
        writeln!(stdout(), "{}", command)?;
        self.ssh.run_command_print_stdout(&command)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
    use std::io::{stdout, Write};
    use std::path::{Path, PathBuf};
    use url::Url;

    use crate::config::Config;
    use crate::file_info::FileInfoTrait;
    use crate::file_info_local::FileInfoLocal;
    use crate::file_info_ssh::FileInfoSSH;
    use crate::file_list::{FileListConfTrait, FileListTrait};
    use crate::file_list_ssh::{FileListSSH, FileListSSHConf};
    use crate::file_service::FileService;

    #[test]
    fn test_file_list_ssh_conf_from_url() {
        let config = Config::init_config().unwrap();
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/"
            .parse()
            .unwrap();
        let conf = FileListSSHConf::from_url(&url, &config).unwrap();
        writeln!(stdout(), "{:?}", conf).unwrap();
        assert_eq!(conf.0.baseurl, url);
        assert_eq!(conf.0.servicetype, FileService::SSH);
    }

    #[test]
    #[ignore]
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
        let flist = FileListSSH::from_conf(conf).unwrap();
        flist.copy_from(&finfo0, &finfo1).unwrap();
        let p = Path::new("/tmp/temp0.txt");
        if p.exists() {
            remove_file(p).unwrap();
        }
    }

    #[test]
    #[ignore]
    fn test_file_list_ssh_copy_to() {
        let config = Config::init_config().unwrap();

        let path: PathBuf = "src/file_list_ssh.rs".parse().unwrap();
        let url: Url = format!("file://{}", path.canonicalize().unwrap().to_string_lossy())
            .parse()
            .unwrap();
        let finfo0 = FileInfoLocal::from_url(&url).unwrap();
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/file_list_ssh.rs"
            .parse()
            .unwrap();
        let finfo1 = FileInfoSSH::from_url(&url).unwrap();

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/".parse().unwrap();
        let conf = FileListSSHConf::from_url(&url, &config).unwrap();
        let flist = FileListSSH::from_conf(conf).unwrap();

        flist.copy_to(&finfo0, &finfo1).unwrap();
        flist.delete(&finfo1).unwrap();
    }
}
