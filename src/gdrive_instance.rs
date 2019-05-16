use google_drive3_fork as drive3;
use yup_oauth2 as oauth2;

use drive3::Drive;
use failure::{err_msg, Error};
use hyper::net::HttpsConnector;
use hyper::Client;
use hyper_native_tls::NativeTlsClient;
use mime::Mime;
use oauth2::{
    Authenticator, ConsoleApplicationSecret, DefaultAuthenticatorDelegate, DiskTokenStorage,
    FlowType,
};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::{create_dir_all, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use url::Url;

use crate::config::Config;

pub type DriveId = String;
type GCClient = Client;
type GCAuthenticator = Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>;
type GCDrive = Drive<GCClient, GCAuthenticator>;

lazy_static! {
    static ref MIME_TYPES: HashMap<&'static str, &'static str> = hashmap! {
        "application/vnd.google-apps.document" => "application/vnd.oasis.opendocument.text",
        "application/vnd.google-apps.presentation" => "application/vnd.oasis.opendocument.presentation",
        "application/vnd.google-apps.spreadsheet" => "application/vnd.oasis.opendocument.spreadsheet",
        "application/vnd.google-apps.drawing" => "image/png",
        "application/vnd.google-apps.site" => "text/plain",
    };
}

lazy_static! {
    static ref UNEXPORTABLE_MIME_TYPES: HashSet<&'static str> = hashset! {
        "application/vnd.google-apps.form",
        "application/vnd.google-apps.map",
    };
}

#[derive(Clone)]
pub struct GDriveInstance {
    pub gdrive: Arc<GCDrive>,
    pub page_size: i32,
    pub max_keys: Option<usize>,
}

impl fmt::Debug for GDriveInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Arc<GDriveInstance>")
    }
}

impl GDriveInstance {
    pub fn new(config: &Config) -> Self {
        GDriveInstance {
            gdrive: Arc::new(GDriveInstance::create_drive(&config).unwrap()),
            page_size: 1000,
            max_keys: None,
        }
    }

    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    fn create_drive_auth(config: &Config) -> Result<GCAuthenticator, Error> {
        let secret_file = File::open(config.gdrive_secret_file.clone())?;
        let secret: ConsoleApplicationSecret = serde_json::from_reader(secret_file)?;
        let secret = secret
            .installed
            .ok_or(err_msg("ConsoleApplicationSecret.installed is None"))?;

        let parent = Path::new(&config.gdrive_token_file)
            .parent()
            .ok_or_else(|| err_msg("No parent"))?;

        if !parent.exists() {
            create_dir_all(parent)?;
        }

        let auth = Authenticator::new(
            &secret,
            DefaultAuthenticatorDelegate,
            Client::with_connector(HttpsConnector::new(NativeTlsClient::new()?)),
            DiskTokenStorage::new(&config.gdrive_token_file)?,
            // Some(FlowType::InstalledInteractive),
            Some(FlowType::InstalledRedirect(8081)),
        );

        Ok(auth)
    }

    /// Creates a drive hub.
    fn create_drive(config: &Config) -> Result<GCDrive, Error> {
        let auth = Self::create_drive_auth(config)?;
        Ok(Drive::new(
            Client::with_connector(HttpsConnector::new(NativeTlsClient::new()?)),
            auth,
        ))
    }

    pub fn get_all_files(&self, get_folders: bool) -> Result<Vec<drive3::File>, Error> {
        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut request = self.gdrive.files()
                .list()
                .param("fields", "nextPageToken,files(name,id,size,mimeType,owners,parents,trashed,modifiedTime,createdTime,viewedByMeTime)")
                .spaces("drive") // TODO: maybe add photos as well
                .corpora("user")
                .page_size(self.page_size)
                .add_scope(drive3::Scope::Full);

            if let Some(token) = page_token {
                request = request.page_token(&token);
            };

            let mut query_chain: Vec<String> = Vec::new();
            if get_folders {
                query_chain.push(r#"mimeType = "application/vnd.google-apps.folder""#.to_string());
            } else {
                query_chain.push(r#"mimeType != "application/vnd.google-apps.folder""#.to_string());
            }
            query_chain.push(format!("trashed = false"));

            let query = query_chain.join(" and ");
            let (_, filelist) = request
                .q(&query)
                .doit()
                .map_err(|e| err_msg(format!("{:#?}", e)))?;

            if let Some(files) = filelist.files {
                all_files.extend(files);
            }

            page_token = filelist.next_page_token;
            if page_token.is_none() {
                break;
            }

            if let Some(max_keys) = self.max_keys {
                if all_files.len() > max_keys {
                    break;
                }
            }
        }
        Ok(all_files)
    }

    pub fn process_list_of_keys<T>(
        &self,
        parents: Option<Vec<DriveId>>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&drive3::File) -> () + Send + Sync,
    {
        let mut n_processed = 0;
        let mut page_token: Option<String> = None;
        loop {
            let mut request = self.gdrive.files()
                .list()
                .param("fields", "nextPageToken,files(name,id,size,mimeType,owners,parents,trashed,modifiedTime,createdTime,viewedByMeTime)")
                .spaces("drive") // TODO: maybe add photos as well
                .corpora("user")
                .page_size(self.page_size)
                .add_scope(drive3::Scope::Full);

            if let Some(token) = page_token {
                request = request.page_token(&token);
            };

            let mut query_chain: Vec<String> = Vec::new();
            if let Some(ref p) = parents {
                let q = p
                    .into_iter()
                    .map(|id| format!("'{}' in parents", id))
                    .collect::<Vec<_>>()
                    .join(" or ");

                query_chain.push(format!("({})", q));
            }
            query_chain.push(format!("trashed = false"));

            let query = query_chain.join(" and ");
            println!("{}", query);
            let (_, filelist) = request
                .q(&query)
                .doit()
                .map_err(|e| err_msg(format!("{:#?}", e)))?;

            if let Some(files) = filelist.files {
                for f in &files {
                    callback(f);
                    n_processed += 1;
                }
            }

            page_token = filelist.next_page_token;
            if page_token.is_none() {
                break;
            }
            println!("{}", n_processed);

            if let Some(max_keys) = self.max_keys {
                if n_processed > max_keys {
                    break;
                }
            }
        }
        Ok(())
    }

    fn get_file_metadata(&self, id: &DriveId) -> Result<drive3::File, Error> {
        self.gdrive
            .files()
            .get(id)
            .param("fields", "id,name,parents,mimeType,webContentLink")
            .add_scope(drive3::Scope::Full)
            .doit()
            .map(|(_response, file)| file)
            .map_err(|e| err_msg(format!("{:#?}", e)))
    }

    pub fn create_directory(&self, directory: &Url, parentid: &DriveId) -> Result<(), Error> {
        let directory_path = directory
            .to_file_path()
            .map_err(|_| err_msg("No file path"))?;
        let directory_name = directory_path
            .file_name()
            .and_then(|d| d.to_str().map(|s| s.to_string()))
            .ok_or_else(|| err_msg("Failed to convert string"))?;
        let new_file = drive3::File {
            name: Some(directory_name),
            mime_type: Some("application/vnd.google-apps.folder".to_string()),
            parents: Some(vec![parentid.clone()]),
            ..Default::default()
        };
        let mime: Mime = "application/octet-stream"
            .parse()
            .map_err(|_| err_msg("bad mimetype"))?;
        let dummy_file = DummyFile::new(&[]);
        self.gdrive
            .files()
            .create(new_file)
            .upload(dummy_file, mime)
            .map_err(|e| err_msg(format!("{:#?}", e)))
            .map(|(_, _)| ())
    }

    pub fn upload(&self, local: &Url, parentid: &DriveId) -> Result<(), Error> {
        let file_path = local.to_file_path().map_err(|_| err_msg("No file path"))?;
        let file_obj = File::open(file_path.clone())?;
        let mime: Mime = "application/octet-stream"
            .parse()
            .map_err(|_| err_msg("bad mimetype"))?;
        let new_file = drive3::File {
            name: file_path
                .as_path()
                .file_name()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string()),
            parents: Some(vec![parentid.clone()]),
            ..Default::default()
        };
        self.gdrive
            .files()
            .create(new_file)
            .upload_resumable(file_obj, mime)
            .map_err(|e| err_msg(format!("{:#?}", e)))
            .map(|(_, _)| ())
    }

    pub fn download(
        &self,
        gdriveid: &DriveId,
        local: &Url,
        mime_type: Option<String>,
    ) -> Result<(), Error> {
        let outpath = local.to_file_path().map_err(|_| err_msg("No file path"))?;
        if let Some(mime) = mime_type.clone() {
            if UNEXPORTABLE_MIME_TYPES.contains::<str>(&mime) {
                return Err(err_msg(format!(
                    "UNEXPORTABLE_FILE: The MIME type of this \
                     file is {:?}, which can not be exported from Drive. Web \
                     content link provided by Drive: {:?}\n",
                    mime,
                    self.get_file_metadata(gdriveid)
                        .ok()
                        .map(|metadata| metadata.web_view_link)
                        .unwrap_or_default()
                )));
            }
        }

        let export_type: Option<&'static str> = mime_type
            .and_then(|ref t| MIME_TYPES.get::<str>(&t))
            .cloned();

        let mut response = match export_type {
            Some(t) => {
                let response = self
                    .gdrive
                    .files()
                    .export(gdriveid, &t)
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| err_msg(format!("{:#?}", e)))?;

                response
            }
            None => {
                let (response, _empty_file) = self
                    .gdrive
                    .files()
                    .get(&gdriveid)
                    .supports_team_drives(false)
                    .param("alt", "media")
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| err_msg(format!("{:#?}", e)))?;
                response
            }
        };

        let mut content: Vec<u8> = Vec::new();
        response.read_to_end(&mut content)?;

        let mut outfile = File::create(outpath)?;
        outfile.write_all(&content)?;

        Ok(())
    }

    pub fn get_directory_map(&self) -> Result<HashMap<String, DirectoryInfo>, Error> {
        let dlist: Vec<_> = self.get_all_files(true)?;
        let mut dmap: HashMap<_, _> = dlist
            .iter()
            .filter_map(|d| {
                if let Some(gdriveid) = d.id.as_ref() {
                    if let Some(name) = d.name.as_ref() {
                        if let Some(parents) = d.parents.as_ref() {
                            if parents.len() > 0 {
                                return Some((
                                    gdriveid.clone(),
                                    DirectoryInfo {
                                        gdriveid: gdriveid.clone(),
                                        name: name.clone(),
                                        parentid: Some(parents[0].clone()),
                                    },
                                ));
                            } else {
                                return Some((
                                    gdriveid.clone(),
                                    DirectoryInfo {
                                        gdriveid: gdriveid.clone(),
                                        name: name.clone(),
                                        parentid: None,
                                    },
                                ));
                            }
                        }
                    }
                }
                None
            })
            .collect();
        let unmatched_parents: HashSet<_> = dmap
            .values()
            .filter_map(|v| {
                v.parentid.as_ref().and_then(|p| match dmap.get(p) {
                    Some(_) => None,
                    None => Some(p.clone()),
                })
            })
            .collect();
        for parent in unmatched_parents {
            let d = self.get_file_metadata(&parent)?;
            println!("pid {} f {:?}", parent, d);
            if let Some(gdriveid) = d.id.as_ref() {
                if let Some(name) = d.name.as_ref() {
                    let parents = if let Some(parents) = d.parents.as_ref() {
                        if parents.len() > 0 {
                            Some(parents[0].clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    let val = DirectoryInfo {
                        gdriveid: gdriveid.clone(),
                        name: name.clone(),
                        parentid: parents,
                    };

                    dmap.entry(gdriveid.clone()).or_insert(val);
                }
            }
        }
        Ok(dmap)
    }

    pub fn get_export_path(
        &self,
        finfo: &drive3::File,
        dirmap: &HashMap<String, DirectoryInfo>,
    ) -> Result<String, Error> {
        let mut fullpath = Vec::new();
        if let Some(name) = finfo.name.as_ref() {
            fullpath.push(name.clone());
        }
        let mut pid = if let Some(parents) = finfo.parents.as_ref() {
            if parents.len() > 0 {
                Some(parents[0].clone())
            } else {
                None
            }
        } else {
            None
        };
        loop {
            pid = if let Some(pid_) = pid.as_ref() {
                if let Some(dinfo) = dirmap.get(pid_) {
                    fullpath.push(dinfo.name.clone());
                    dinfo.parentid.clone()
                } else {
                    println!("pid {}", pid_);
                    None
                }
            } else {
                None
            };
            if pid.is_none() {
                break;
            }
        }
        let fullpath: Vec<_> = fullpath.into_iter().rev().collect();
        Ok(fullpath.join("/"))
    }
}

pub struct DirectoryInfo {
    pub gdriveid: DriveId,
    pub name: String,
    pub parentid: Option<DriveId>,
}

struct DummyFile {
    cursor: u64,
    data: Vec<u8>,
}

impl DummyFile {
    fn new(data: &[u8]) -> DummyFile {
        DummyFile {
            cursor: 0,
            data: Vec::from(data),
        }
    }
}

impl Seek for DummyFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position: i64 = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.data.len() as i64 - offset,
            SeekFrom::Current(offset) => self.cursor as i64 + offset,
        };

        if position < 0 {
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        } else {
            self.cursor = position as u64;
            Ok(self.cursor)
        }
    }
}

impl Read for DummyFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.data.len() - self.cursor as usize;
        let copied = cmp::min(buf.len(), remaining);

        if copied > 0 {
            buf[..]
                .copy_from_slice(&self.data[self.cursor as usize..self.cursor as usize + copied]);
        }

        self.cursor += copied as u64;
        Ok(copied)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::gdrive_instance::GDriveInstance;

    #[test]
    fn test_create_drive() {
        let config = Config::new();
        let gdrive = GDriveInstance::new(&config)
            .with_max_keys(100)
            .with_page_size(100);
        let list = gdrive.get_all_files(None).unwrap();
        assert_eq!(list.len(), 200);
    }
}
