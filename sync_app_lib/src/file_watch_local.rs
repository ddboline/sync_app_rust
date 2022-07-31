use anyhow::{format_err, Error};
use notify::{watcher, DebouncedEvent, INotifyWatcher, RecursiveMode, Watcher};
use stack_string::{format_sstr, StackString};
use std::{
    convert::TryInto,
    path::Path,
    sync::{
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc,
    },
    time::Duration,
};
use tokio::time::sleep;

use crate::{
    file_info::FileStat,
    file_info_local::{_get_md5sum, _get_sha1sum, _get_stat},
    models::FileInfoCache,
    pgpool::PgPool,
};

pub struct FileWatchLocal {
    recv: Arc<Receiver<DebouncedEvent>>,
    _watcher: Arc<INotifyWatcher>,
}

impl FileWatchLocal {
    pub fn new(paths: &[&Path]) -> Result<Self, Error> {
        let (send, recv) = channel();
        let mut watcher = watcher(send, Duration::from_millis(10))?;
        for watch_path in paths {
            watcher.watch(watch_path, RecursiveMode::Recursive)?;
        }
        Ok(Self {
            recv: Arc::new(recv),
            _watcher: Arc::new(watcher),
        })
    }

    fn get_local_info(p: &Path) -> Result<(String, String, FileStat), Error> {
        let md5sum = _get_md5sum(p)?;
        let sha1sum = _get_sha1sum(p)?;
        let stat = _get_stat(p)?;
        Ok((md5sum, sha1sum, stat))
    }

    pub async fn run(&self, pool: &PgPool) -> Result<(), Error> {
        loop {
            match self.recv.try_recv() {
                Ok(DebouncedEvent::NoticeWrite(p) | DebouncedEvent::Write(p)) => {
                    let (md5sum, sha1sum, stat) = Self::get_local_info(&p)?;
                    for mut info in FileInfoCache::get_local_by_path(&p, pool).await? {
                        info.filestat_st_mtime = stat.st_mtime as i32;
                        info.filestat_st_size = stat.st_size as i32;
                        info.md5sum = md5sum.clone().try_into().ok();
                        info.sha1sum = sha1sum.clone().try_into().ok();
                        info.insert(pool).await?;
                    }
                }
                Ok(DebouncedEvent::Remove(p) | DebouncedEvent::NoticeRemove(p)) => {
                    for info in FileInfoCache::get_local_by_path(&p, pool).await? {
                        info.delete(pool).await?;
                    }
                }
                Ok(DebouncedEvent::Create(p)) => {}
                Ok(DebouncedEvent::Rename(p0, p1)) => {}
                Err(TryRecvError::Empty)
                | Ok(
                    DebouncedEvent::Chmod(_) | DebouncedEvent::Rescan | DebouncedEvent::Error(_, _),
                ) => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(format_err!("Watch disconnected"));
                }
            }
        }
    }

    fn test(
        &self,
        test_send: Sender<StackString>,
        test_control: Receiver<Option<()>>,
    ) -> Result<(), Error> {
        loop {
            match self.recv.try_recv() {
                Ok(DebouncedEvent::NoticeWrite(p) | DebouncedEvent::Write(p)) => {
                    let p = p.to_string_lossy();
                    test_send.send(format_sstr!("write event {p}"))?;
                }
                Ok(DebouncedEvent::Remove(p) | DebouncedEvent::NoticeRemove(p)) => {
                    let p = p.to_string_lossy();
                    test_send.send(format_sstr!("remove event {p}"))?;
                }
                Ok(DebouncedEvent::Create(p)) => {
                    let p = p.to_string_lossy();
                    test_send.send(format_sstr!("create event {p}"))?;
                }
                Ok(DebouncedEvent::Rename(p0, p1)) => {
                    let p0 = p0.to_string_lossy();
                    let p1 = p1.to_string_lossy();
                    test_send.send(format_sstr!("rename event {p0} {p1}"))?;
                }
                Err(TryRecvError::Empty)
                | Ok(
                    DebouncedEvent::Chmod(_) | DebouncedEvent::Rescan | DebouncedEvent::Error(_, _),
                ) => {}
                Err(TryRecvError::Disconnected) => {
                    return Err(format_err!("Watch disconnected"));
                }
            }
            if let Ok(Some(())) = test_control.try_recv() {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use parking_lot::Mutex;
    use stack_string::format_sstr;
    use std::{
        sync::{mpsc::channel, Arc},
        time::Duration,
    };
    use tempfile::TempDir;
    use tokio::{
        fs,
        task::{spawn_blocking, JoinHandle},
        time::sleep,
    };

    use crate::file_watch_local::FileWatchLocal;

    #[tokio::test]
    async fn test_watch_directory() -> Result<(), Error> {
        let temp_dir = TempDir::new()?;
        let temp_file = temp_dir.as_ref().join("test_file.txt");
        let new_temp_file = temp_dir.as_ref().join("test_file_new.txt");

        println!(
            "files {} {}",
            temp_file.to_string_lossy(),
            new_temp_file.to_string_lossy()
        );

        let collect: Arc<Mutex<Vec<_>>> = Arc::new(Mutex::new(Vec::new()));

        let (send, recv) = channel();
        let (control_send, control_recv) = channel();

        let collect_task: JoinHandle<Result<(), Error>> = spawn_blocking({
            let collect = collect.clone();
            move || {
                while let Ok(result) = recv.recv() {
                    collect.lock().push(result);
                }
                Ok(())
            }
        });

        let task: JoinHandle<Result<(), Error>> = spawn_blocking(move || {
            let watch = FileWatchLocal::new(&[temp_dir.as_ref()])?;
            watch.test(send, control_recv)?;
            Ok(())
        });

        let buffer = "TEST FILE";
        fs::write(&temp_file, buffer.as_bytes()).await?;
        fs::rename(&temp_file, &new_temp_file).await?;

        loop {
            sleep(Duration::from_secs(1)).await;
            let collect_len = collect.lock().len();
            println!("collect {}", collect_len);
            if collect_len > 2 {
                break;
            }
        }

        control_send.send(Some(()))?;

        task.await.unwrap()?;
        collect_task.await.unwrap()?;

        println!("collect {}", collect.lock().len());
        for ent in collect.lock().iter() {
            println!("ent {ent}");
        }
        assert_eq!(collect.lock().len(), 2);
        let temp_file = temp_file.to_string_lossy();
        let new_temp_file = new_temp_file.to_string_lossy();
        if let Some(x) = collect.lock().pop() {
            assert_eq!(x, format_sstr!("remove event {new_temp_file}"))
        }
        if let Some(x) = collect.lock().pop() {
            assert_eq!(x, format_sstr!("rename event {temp_file} {new_temp_file}"))
        }

        assert!(false);
        Ok(())
    }
}
