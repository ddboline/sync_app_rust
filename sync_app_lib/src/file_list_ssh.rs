use anyhow::{format_err, Error};
use async_trait::async_trait;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::create_dir_all;
use std::io::{stdout, Write};
use std::path::Path;
use tokio::task::spawn_blocking;
use url::Url;

use crate::config::Config;
use crate::file_info::{FileInfo, FileInfoTrait, ServiceSession};
use crate::file_list::{FileList, FileListTrait};
use crate::file_service::FileService;
use crate::pgpool::PgPool;
use crate::ssh_instance::SSHInstance;

#[derive(Clone, Debug)]
pub struct FileListSSH {
    pub flist: FileList,
    pub ssh: SSHInstance,
}

impl FileListSSH {
    pub async fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "ssh" {
            let basepath = Path::new(url.path()).to_path_buf();
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
            let flist = FileList::new(
                url.clone(),
                basepath,
                config.clone(),
                FileService::SSH,
                session.parse()?,
                HashMap::new(),
                pool.clone(),
            );
            let url = url.clone();
            let ssh = spawn_blocking(move || SSHInstance::from_url(&url)).await??;

            Ok(Self { flist, ssh })
        } else {
            Err(format_err!("Wrong scheme"))
        }
    }
}

#[async_trait]
impl FileListTrait for FileListSSH {
    fn get_baseurl(&self) -> &Url {
        self.flist.get_baseurl()
    }
    fn set_baseurl(&mut self, baseurl: Url) {
        self.flist.set_baseurl(baseurl);
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
        self.flist.get_filemap()
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.flist.with_list(filelist)
    }

    // Copy operation where the origin (finfo0) has the same servicetype as self
    async fn copy_from(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
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
    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
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

    async fn move_file(
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

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
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

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let baseurl = self.get_baseurl();

        let path = self.get_basepath().to_string_lossy();
        let user_host = self.ssh.get_ssh_username_host()?;
        let command = format!("sync-app-rust index -u file://{}", path);
        self.ssh.run_command_ssh(&command)?;

        let command = format!("sync-app-rust ser -u file://{}", path);

        let url_prefix = format!("ssh://{}", user_host);

        self.ssh
            .run_command_stream_stdout(&command)?
            .into_iter()
            .map(|l| {
                let mut finfo: FileInfo = serde_json::from_str(&l)?;
                finfo.servicetype = FileService::SSH;
                finfo.urlname = finfo
                    .urlname
                    .and_then(|u| u.as_str().replace("file://", &url_prefix).parse().ok());
                finfo.serviceid = Some(baseurl.clone().into_string().into());
                finfo.servicesession = baseurl.as_str().parse().ok();
                Ok(finfo)
            })
            .collect()
    }

    async fn print_list(&self) -> Result<(), Error> {
        let path = self.get_basepath().to_string_lossy();
        let command = format!("sync-app-rust ls -u file://{}", path);
        writeln!(stdout(), "{}", command)?;
        self.ssh.run_command_print_stdout(&command)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use std::fs::remove_file;
    use std::io::{stdout, Write};
    use std::path::{Path, PathBuf};
    use url::Url;

    use crate::config::Config;
    use crate::file_info::FileInfoTrait;
    use crate::file_info_local::FileInfoLocal;
    use crate::file_info_ssh::FileInfoSSH;
    use crate::file_list::FileListTrait;
    use crate::file_list_ssh::FileListSSH;
    use crate::file_service::FileService;
    use crate::pgpool::PgPool;

    #[test]
    #[ignore]
    fn test_file_list_ssh_conf_from_url() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/".parse()?;
        let conf = FileListSSH::from_url(&url, &config, &pool)?;
        writeln!(stdout(), "{:?}", conf)?;
        assert_eq!(conf.get_baseurl(), &url);
        assert_eq!(conf.get_servicetype(), FileService::SSH);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_file_list_ssh_copy_from() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/temp0.txt".parse()?;
        let finfo0 = FileInfoSSH::from_url(&url)?;
        let url: Url = "file:///tmp/temp0.txt".parse()?;
        let finfo1 = FileInfoLocal::from_url(&url)?;

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/".parse()?;
        let flist = FileListSSH::from_url(&url, &config, &pool)?;
        flist.copy_from(&finfo0, &finfo1).await?;
        let p = Path::new("/tmp/temp0.txt");
        if p.exists() {
            remove_file(p)?;
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_file_list_ssh_copy_to() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);

        let path: PathBuf = "src/file_list_ssh.rs".parse()?;
        let url: Url = format!("file://{}", path.canonicalize()?.to_string_lossy()).parse()?;
        let finfo0 = FileInfoLocal::from_url(&url)?;
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/file_list_ssh.rs".parse()?;
        let finfo1 = FileInfoSSH::from_url(&url)?;

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/".parse()?;
        let flist = FileListSSH::from_url(&url, &config, &pool)?;

        flist.copy_to(&finfo0, &finfo1).await?;
        flist.delete(&finfo1).await?;
        Ok(())
    }
}
