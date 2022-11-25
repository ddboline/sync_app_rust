use anyhow::{format_err, Error};
use async_trait::async_trait;
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fs::create_dir_all, path::Path};
use stdout_channel::StdoutChannel;
use url::Url;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoInner, FileInfoTrait, ServiceSession},
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    pgpool::PgPool,
    ssh_instance::SSHInstance,
};

#[derive(Clone, Debug)]
pub struct FileListSSH {
    pub flist: FileList,
    pub ssh: SSHInstance,
}

impl FileListSSH {
    /// # Errors
    /// Return error if db query fails
    pub async fn from_url(url: &Url, config: &Config, pool: &PgPool) -> Result<Self, Error> {
        if url.scheme() == "ssh" {
            let basepath = Path::new(url.path()).to_path_buf();
            let host = url.host_str().ok_or_else(|| format_err!("Parse error"))?;
            let port = url.port().unwrap_or(22);
            let session = format_sstr!(
                "ssh://{}@{}:{}{}",
                url.username(),
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
            let ssh = SSHInstance::from_url(&url).await?;

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

    fn get_filemap(&self) -> &HashMap<StackString, FileInfo> {
        self.flist.get_filemap()
    }

    fn with_list(&mut self, filelist: Vec<FileInfo>) {
        self.flist.with_list(filelist);
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
            let url0 = &finfo0.get_finfo().urlname;
            let path0 = Path::new(url0.path()).to_string_lossy();

            let parent_dir = finfo1
                .filepath
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?;
            if !parent_dir.exists() {
                create_dir_all(parent_dir)?;
            }

            self.ssh
                .run_scp(
                    &self.ssh.get_ssh_str(&path0),
                    finfo1.filepath.to_string_lossy().as_ref(),
                )
                .await
        } else {
            Err(format_err!(
                "Invalid types {} {}",
                finfo0.servicetype,
                finfo1.servicetype
            ))
        }
    }

    // Copy operation where the destination (finfo0) has the same servicetype as
    // self
    async fn copy_to(
        &self,
        finfo0: &dyn FileInfoTrait,
        finfo1: &dyn FileInfoTrait,
    ) -> Result<(), Error> {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();
        if finfo0.servicetype == FileService::Local && finfo1.servicetype == FileService::SSH {
            let url1 = &finfo1.get_finfo().urlname;
            let path1 = Path::new(url1.path()).to_string_lossy();

            let parent_dir = finfo1
                .filepath
                .parent()
                .ok_or_else(|| format_err!("No parent directory"))?
                .to_string_lossy()
                .replace(' ', r#"\ "#);
            let command = format_sstr!("mkdir -p {parent_dir}");
            self.ssh.run_command_ssh(&command).await?;

            self.ssh
                .run_scp(
                    finfo0.filepath.to_string_lossy().as_ref(),
                    &self.ssh.get_ssh_str(&path1),
                )
                .await
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
        let url0 = &finfo0.get_finfo().urlname;
        let url1 = &finfo1.get_finfo().urlname;

        if url0.username() != url1.username() || url0.host_str() != url1.host_str() {
            return Ok(());
        }

        let path0 = Path::new(url0.path())
            .to_string_lossy()
            .replace(' ', r#"\ "#);
        let path1 = Path::new(url1.path())
            .to_string_lossy()
            .replace(' ', r#"\ "#);
        let command = format_sstr!("mv {path0} {path1}");
        self.ssh.run_command_ssh(&command).await
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        let url = &finfo.get_finfo().urlname;
        let path = Path::new(url.path())
            .to_string_lossy()
            .replace(' ', r#"\ "#);
        let command = format_sstr!("rm {path}");
        self.ssh.run_command_ssh(&command).await
    }

    async fn fill_file_list(&self) -> Result<Vec<FileInfo>, Error> {
        let path = self.get_basepath().to_string_lossy();
        let user_host = self.ssh.get_ssh_username_host();
        let user_host = user_host
            .iter()
            .last()
            .ok_or_else(|| format_err!("No hostname"))?;
        let command = format_sstr!(r#"sync-app-rust index -u file://{path}"#);
        self.ssh.run_command_stream_stdout(&command).await?;
        let command = format_sstr!(r#"sync-app-rust count -u file://{path}"#);
        let output = self.ssh.run_command_stream_stdout(&command).await?;
        let output = output.trim();
        let expected_count: usize = output
            .split('\t')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if expected_count == 0 {
            println!("path empty {path}");
            Ok(Vec::new())
        } else {
            println!("expected_count {expected_count}");
            let mut offset = 0;
            let limit = 1000;

            let mut items = Vec::new();

            while items.len() != expected_count {
                if items.len() > expected_count {
                    let observed = items.len();
                    return Err(format_err!("inconsistency {observed} {expected_count}"));
                }
                let command = format_sstr!(
                    r#"sync-app-rust ser -u file://{path} --offset {offset} --limit {limit} --expected {expected_count}"#
                );
                let output = self.ssh.run_command_stream_stdout(&command).await?;
                let output = output.trim();

                let url_prefix = format_sstr!("ssh://{user_host}");
                let baseurl = self.get_baseurl().clone();

                let result: Result<Vec<_>, Error> = output
                    .split('\n')
                    .map(|line| {
                        let baseurl = baseurl.as_str();
                        let mut finfo: FileInfoInner = serde_json::from_str(line)?;
                        finfo.servicetype = FileService::SSH;
                        finfo.urlname = finfo
                            .urlname
                            .as_str()
                            .replace("file://", &url_prefix)
                            .parse()?;
                        finfo.serviceid = baseurl.into();
                        finfo.servicesession = baseurl.parse()?;
                        Ok(FileInfo::from_inner(finfo))
                    })
                    .collect();
                let result = result?;
                items.extend_from_slice(&result);
                if result.is_empty() {
                    let observed = items.len();
                    return Err(format_err!(
                        "no results returned {observed} {expected_count}"
                    ));
                }
                offset += result.len();
            }

            if items.len() == expected_count {
                Ok(items)
            } else {
                Err(format_err!(
                    "{} {} Expected {} doesn't match actual count {}",
                    self.get_servicetype(),
                    self.get_servicesession().as_str(),
                    expected_count,
                    items.len()
                ))
            }
        }
    }

    async fn print_list(&self, stdout: &StdoutChannel<StackString>) -> Result<(), Error> {
        let path = self.get_basepath().to_string_lossy();
        let command = format_sstr!("sync-app-rust ls -u file://{path}");
        stdout.send(&command);
        self.ssh.run_command_print_stdout(&command).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use log::debug;
    use stack_string::format_sstr;
    use std::{
        fs::remove_file,
        path::{Path, PathBuf},
    };
    use url::Url;

    use crate::{
        config::Config, file_info_local::FileInfoLocal, file_info_ssh::FileInfoSSH,
        file_list::FileListTrait, file_list_ssh::FileListSSH, file_service::FileService,
        pgpool::PgPool,
    };

    #[tokio::test]
    #[ignore]
    async fn test_file_list_ssh_conf_from_url() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url);
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/".parse()?;
        let conf = FileListSSH::from_url(&url, &config, &pool).await?;
        debug!("{:?}", conf);
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
        let flist = FileListSSH::from_url(&url, &config, &pool).await?;
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
        let url: Url = format_sstr!("file://{}", path.canonicalize()?.to_string_lossy()).parse()?;
        let finfo0 = FileInfoLocal::from_url(&url)?;
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/file_list_ssh.rs".parse()?;
        let finfo1 = FileInfoSSH::from_url(&url)?;

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/tmp/".parse()?;
        let flist = FileListSSH::from_url(&url, &config, &pool).await?;

        flist.copy_to(&finfo0, &finfo1).await?;
        flist.delete(&finfo1).await?;
        Ok(())
    }
}
