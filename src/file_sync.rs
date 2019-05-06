use failure::Error;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use crate::file_info::{FileInfo, FileInfoTrait};
use crate::file_list::{replace_baseurl, FileListTrait};
use crate::file_service::FileService;
use crate::map_result_vec;

#[derive(Debug)]
pub enum FileSyncMode {
    OutputFile(PathBuf),
    Full,
}

impl Default for FileSyncMode {
    fn default() -> FileSyncMode {
        FileSyncMode::Full
    }
}

#[derive(Default, Debug)]
pub struct FileSync {
    pub mode: FileSyncMode,
}

impl FileSync {
    pub fn new(mode: FileSyncMode) -> FileSync {
        FileSync { mode }
    }

    pub fn compare_lists<T, U>(&self, flist0: &T, flist1: &U) -> Result<(), Error>
    where
        T: FileListTrait + Send + Sync,
        U: FileListTrait + Send + Sync,
    {
        let conf0 = flist0.get_conf();
        let conf1 = flist1.get_conf();
        println!("{:?} {:?}", conf0.baseurl, conf1.baseurl);
        let list_a_not_b: Vec<_> = flist0
            .get_filemap()
            .iter()
            .filter_map(|(k, finfo0)| match flist1.get_filemap().get(k) {
                Some(finfo1) => {
                    if self.compare_objects(finfo0, finfo1) {
                        Some((finfo0.clone(), finfo1.clone()))
                    } else {
                        None
                    }
                }
                None => {
                    if let Some(url0) = finfo0.urlname.as_ref() {
                        if let Ok(url1) = replace_baseurl(&url0, &conf0.baseurl, &conf1.baseurl) {
                            if url1.as_str().contains(conf1.baseurl.as_str()) {
                                let finfo1 = FileInfo {
                                    filename: k.clone(),
                                    urlname: Some(url1),
                                    servicesession: Some(conf1.servicesession.clone()),
                                    servicetype: conf1.servicetype.clone(),
                                    serviceid: Some(conf1.serviceid.clone()),
                                    ..Default::default()
                                };
                                println!("{:?}", conf1.baseurl);
                                Some((finfo0.clone(), finfo1.clone()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            })
            .collect();
        let list_b_not_a: Vec<_> = flist1
            .get_filemap()
            .iter()
            .filter_map(|(k, finfo1)| match flist0.get_filemap().get(k) {
                Some(_) => None,
                None => {
                    if let Some(url1) = finfo1.urlname.as_ref() {
                        if let Ok(url0) = replace_baseurl(&url1, &conf1.baseurl, &conf0.baseurl) {
                            if url0.as_str().contains(conf0.baseurl.as_str()) {
                                let finfo0 = FileInfo {
                                    filename: k.clone(),
                                    urlname: Some(url0),
                                    servicesession: Some(conf0.servicesession.clone()),
                                    servicetype: conf0.servicetype.clone(),
                                    serviceid: Some(conf0.serviceid.clone()),
                                    ..Default::default()
                                };
                                println!("{:?}", conf0.baseurl);
                                Some((finfo0.clone(), finfo1.clone()))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            })
            .collect();
        match &self.mode {
            FileSyncMode::Full => {
                let result: Vec<Result<_, Error>> = list_a_not_b
                    .par_iter()
                    .filter_map(|(f0, f1)| {
                        if conf1.servicetype == FileService::Local {
                            Some(flist0.download_file(&f0, &f1))
                        } else if conf0.servicetype == FileService::Local {
                            Some(flist0.upload_file(&f0, &f1))
                        } else {
                            None
                        }
                    })
                    .collect();
                map_result_vec(result)?;
                let result: Vec<Result<_, Error>> = list_b_not_a
                    .par_iter()
                    .filter_map(|(f0, f1)| {
                        if conf0.servicetype == FileService::Local {
                            Some(flist1.download_file(&f1, &f0))
                        } else if conf1.servicetype == FileService::Local {
                            Some(flist1.upload_file(&f1, &f0))
                        } else {
                            None
                        }
                    })
                    .collect();
                map_result_vec(result)?;
            }
            FileSyncMode::OutputFile(fname) => {
                let mut f = File::create(fname)?;
                for (f0, f1) in list_a_not_b.iter().chain(list_b_not_a.iter()) {
                    println!("{:?}", f0);
                    println!("{:?}", f1);
                    if let Some(u0) = f0.urlname.as_ref() {
                        if let Some(u1) = f1.urlname.as_ref() {
                            write!(f, "{} {}\n", u0, u1)?;
                        }
                    }
                }
            }
        };
        Ok(())
    }

    pub fn compare_objects<T, U>(&self, finfo0: &T, finfo1: &U) -> bool
    where
        T: FileInfoTrait + Send + Sync,
        U: FileInfoTrait + Send + Sync,
    {
        let finfo0 = finfo0.get_finfo();
        let finfo1 = finfo1.get_finfo();

        let use_sha1 = (finfo0.servicetype == FileService::OneDrive)
            || (finfo1.servicetype == FileService::OneDrive);
        if finfo0.filename != finfo1.filename {
            return false;
        }
        if let Some(fstat0) = finfo0.filestat.as_ref() {
            if let Some(fstat1) = finfo1.filestat.as_ref() {
                if fstat0.st_mtime > fstat1.st_mtime {
                    return true;
                }
                if fstat0.st_size != fstat1.st_size {
                    return true;
                }
            }
        }
        if use_sha1 {
            if let Some(sha0) = finfo0.sha1sum.as_ref() {
                if let Some(sha1) = finfo1.sha1sum.as_ref() {
                    if sha0 == sha1 {
                        return false;
                    }
                }
            }
        } else {
            if let Some(md50) = finfo0.md5sum.as_ref() {
                if let Some(md51) = finfo1.md5sum.as_ref() {
                    if md50 == md51 {
                        return false;
                    }
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use rusoto_s3::{Object, Owner};
    use std::env::current_dir;
    use std::io::Read;
    use std::io::{Seek, SeekFrom};
    use std::path::Path;
    use tempfile::NamedTempFile;

    use crate::file_info::{FileInfo, ServiceId, ServiceSession};
    use crate::file_info_local::FileInfoLocal;
    use crate::file_info_s3::FileInfoS3;
    use crate::file_list_local::{FileListLocal, FileListLocalConf};
    use crate::file_list_s3::{FileListS3, FileListS3Conf};
    use crate::file_sync::{FileSync, FileSyncMode};

    #[test]
    fn test_compare_objects() {
        let outfile = NamedTempFile::new().unwrap();
        let fsync = FileSync::new(FileSyncMode::OutputFile(outfile.path().to_path_buf()));

        let filepath = Path::new("src/file_sync.rs").canonicalize().unwrap();
        let serviceid: ServiceId = filepath.to_str().unwrap().to_string().into();
        let servicesession: ServiceSession = filepath.to_str().unwrap().parse().unwrap();
        let finfo0 =
            FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession)).unwrap();
        println!("{:?}", finfo0);
        let mut finfo1 = finfo0.clone();
        finfo1.0.md5sum = Some("51e3cc2c6f64d24ff55fae262325edee".parse().unwrap());
        let mut fstat = finfo1.0.filestat.unwrap();
        fstat.st_mtime += 100;
        fstat.st_size += 100;
        finfo1.0.filestat = Some(fstat);
        println!("{:?}", finfo1);
        assert!(fsync.compare_objects(&finfo0, &finfo1));

        let test_owner = Owner {
            display_name: Some("me".to_string()),
            id: Some("8675309".to_string()),
        };
        let test_object = Object {
            e_tag: Some(r#""6f90ebdaabef92a9f76be131037f593b""#.to_string()),
            key: Some("src/file_sync.rs".to_string()),
            last_modified: Some("2019-05-01T00:00:00+00:00".to_string()),
            owner: Some(test_owner),
            size: Some(100),
            storage_class: Some("Standard".to_string()),
        };

        let finfo2 = FileInfoS3::from_object("test_bucket", test_object).unwrap();
        println!("{:?}", finfo2);
        assert!(fsync.compare_objects(&finfo0, &finfo2));
    }

    #[test]
    fn test_compare_lists() {
        let mut outfile = NamedTempFile::new().unwrap();
        let fsync = FileSync::new(FileSyncMode::OutputFile(outfile.path().to_path_buf()));

        let filepath = Path::new("src/file_sync.rs").canonicalize().unwrap();
        let serviceid: ServiceId = filepath.to_str().unwrap().to_string().into();
        let servicesession: ServiceSession = filepath.to_str().unwrap().parse().unwrap();
        let finfo0 =
            FileInfoLocal::from_path(&filepath, Some(serviceid), Some(servicesession)).unwrap();
        println!("{:?}", finfo0);

        let flist0conf = FileListLocalConf::new(current_dir().unwrap()).unwrap();
        let flist0 = FileListLocal::from_conf(flist0conf).with_list(&[finfo0.0]);

        let flist1conf = FileListS3Conf::new("test_bucket").unwrap();
        let flist1 = FileListS3::from_conf(flist1conf.0, None, None);

        fsync.compare_lists(&flist0, &flist1).unwrap();

        outfile.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer = String::new();
        let bytes_read = outfile.read_to_string(&mut buffer).unwrap();

        assert!(bytes_read > 0);

        println!("{}", buffer.trim());
        println!("{}", current_dir().unwrap().to_str().unwrap());
        assert_eq!(
            buffer.trim(),
            format!(
                "file://{}/src/file_sync.rs s3://test_bucket/src/file_sync.rs",
                current_dir().unwrap().to_str().unwrap()
            )
        );
    }
}
