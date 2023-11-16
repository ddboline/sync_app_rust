use anyhow::{format_err, Error};
use async_trait::async_trait;
use futures::TryStreamExt;
use log::{debug, error};
use rand::{thread_rng, RngCore};
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fs::create_dir_all, path::Path};
use stdout_channel::StdoutChannel;
use tokio::{fs::remove_file, process::Command};
use url::Url;

use crate::{
    config::Config,
    file_info::{FileInfo, FileInfoInner, FileInfoTrait, ServiceSession},
    file_list::{FileList, FileListTrait},
    file_service::FileService,
    models::FileInfoCache,
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
            let host = if port == 22 {
                host.into()
            } else {
                format_sstr!("{host}:{port}")
            };
            let username = url.username();

            let session = format_sstr!("ssh://{username}@{host}{}", basepath.to_string_lossy());
            let flist = FileList::new(
                url.clone(),
                basepath,
                config.clone(),
                FileService::SSH,
                session.parse()?,
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
                .replace(' ', r"\ ");
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
        let path0 = Path::new(url0.path()).to_string_lossy().replace(' ', r"\ ");
        let path1 = Path::new(url1.path()).to_string_lossy().replace(' ', r"\ ");
        let command = format_sstr!("mv {path0} {path1}");
        self.ssh.run_command_ssh(&command).await
    }

    async fn delete(&self, finfo: &dyn FileInfoTrait) -> Result<(), Error> {
        let finfo = finfo.get_finfo();
        let url = &finfo.get_finfo().urlname;
        let path = Path::new(url.path()).to_string_lossy().replace(' ', r"\ ");
        let command = format_sstr!("rm {path}");
        self.ssh.run_command_ssh(&command).await
    }

    async fn update_file_cache(&self) -> Result<usize, Error> {
        let path = self.get_basepath().to_string_lossy();
        let user_host = self.ssh.get_ssh_username_host();
        let user_host = user_host
            .iter()
            .last()
            .ok_or_else(|| format_err!("No hostname"))?;
        let url_prefix = format_sstr!("ssh://{user_host}");
        let baseurl = self.get_baseurl().clone();
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

        let pool = self.get_pool();
        let cached_urls: HashMap<StackString, _> = FileInfoCache::get_all_cached(
            self.get_servicesession().as_str(),
            self.get_servicetype().to_str(),
            &pool,
            false,
        )
        .await?
        .map_ok(|f| (f.urlname.clone(), f))
        .try_collect()
        .await?;
        debug!("expected {}", cached_urls.len());

        let mut actual_length = 0;

        if expected_count == 0 {
            println!("path empty {path}");
            Ok(0)
        } else {
            for _ in 0..5 {
                let randint = thread_rng().next_u32();
                let tmp_file = format_sstr!("/tmp/{user_host}_{randint}.json");
                let command = format_sstr!(
                    r#"sync-app-rust ser -u file://{path} -f {tmp_file} && gzip {tmp_file}"#
                );
                self.ssh.run_command_stream_stdout(&command).await?;
                let tmp_file = format_sstr!("{tmp_file}.gz");

                self.ssh
                    .run_scp(&self.ssh.get_ssh_str(&tmp_file), &tmp_file)
                    .await?;
                let command = format_sstr!("rm {tmp_file}");
                self.ssh.run_command_stream_stdout(&command).await?;

                let process = Command::new("gzip")
                    .args(["-dc", &tmp_file])
                    .output()
                    .await?;
                let output = if process.status.success() {
                    StackString::from_utf8_vec(process.stdout)?
                } else {
                    error!("{}", StackString::from_utf8_lossy(&process.stderr));
                    return Err(format_err!("Process failed"));
                };
                remove_file(&tmp_file).await?;
                let result: Result<Vec<_>, Error> = output
                    .split('\n')
                    .map(|line| {
                        if line.is_empty() {
                            Ok(None)
                        } else {
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
                            Ok(Some(FileInfo::from_inner(finfo)))
                        }
                    })
                    .filter_map(Result::transpose)
                    .collect();
                let items = result?;

                println!("result {expected_count} {}", items.len());

                if items.len() == expected_count {
                    let mut updated = 0;
                    for item in items {
                        let info: FileInfoCache = item.into();
                        if let Some(existing) = cached_urls.get(&info.urlname) {
                            if existing.deleted_at.is_none()
                                && existing.filestat_st_size == info.filestat_st_size
                            {
                                continue;
                            }
                        }
                        updated += info.upsert(pool).await?;
                    }
                    return Ok(updated);
                }
                actual_length = items.len();
            }
            Err(format_err!(
                "{} {} Expected {expected_count} doesn't match actual count {actual_length}",
                self.get_servicetype(),
                self.get_servicesession().as_str(),
            ))
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
        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/pkgs.txt".parse()?;
        let finfo0 = FileInfoSSH::from_url(&url)?;
        let url: Url = "file:///tmp/pkgs.txt".parse()?;
        let finfo1 = FileInfoLocal::from_url(&url)?;

        let url: Url = "ssh://ubuntu@cloud.ddboline.net/home/ubuntu/".parse()?;
        let flist = FileListSSH::from_url(&url, &config, &pool).await?;
        flist.copy_from(&finfo0, &finfo1).await?;
        let p = Path::new("/tmp/pkgs.txt");
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
