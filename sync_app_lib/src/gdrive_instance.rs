use drive3::Drive;
use failure::{err_msg, format_err, Error};
use google_drive3_fork as drive3;
use hyper::net::HttpsConnector;
use hyper::Client;
use hyper_native_tls::NativeTlsClient;
use lazy_static::lazy_static;
use log::debug;
use maplit::{hashmap, hashset};
use mime::Mime;
use oauth2::{
    Authenticator, ConsoleApplicationSecret, DefaultAuthenticatorDelegate, DiskTokenStorage,
    FlowType,
};
use percent_encoding::percent_decode;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{create_dir_all, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::string::ToString;
use url::Url;
use yup_oauth2 as oauth2;

use crate::config::Config;
use crate::directory_info::DirectoryInfo;
use crate::exponential_retry;

type GCClient = Client;
type GCAuthenticator = Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>;
type GCDrive = Drive<GCClient, GCAuthenticator>;

lazy_static! {
    static ref MIME_TYPES: HashMap<&'static str, &'static str> = hashmap! {
        "application/vnd.google-apps.document" => "application/vnd.oasis.opendocument.text",
        "application/vnd.google-apps.presentation" => "application/pdf",
        "application/vnd.google-apps.spreadsheet" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.google-apps.drawing" => "image/png",
        "application/vnd.google-apps.site" => "text/plain",
    };
}

lazy_static! {
    static ref UNEXPORTABLE_MIME_TYPES: HashSet<&'static str> = hashset! {
        "application/vnd.google-apps.form",
        "application/vnd.google-apps.map",
        "application/vnd.google-apps.folder",
    };
}

lazy_static! {
    static ref EXTENSIONS: HashMap<&'static str, &'static str> = hashmap! {
        "application/vnd.oasis.opendocument.text" => "odt",
        "image/png" => "png",
        "application/pdf" => "pdf",
        "image/jpeg" => "jpg",
        "text/x-csrc" => "C",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
    };
}

#[derive(Clone)]
pub struct GDriveInstance {
    pub gdrive: Rc<GCDrive>,
    pub page_size: i32,
    pub max_keys: Option<usize>,
    pub session_name: String,
    pub start_page_token: Option<String>,
    pub start_page_token_filename: String,
}

impl fmt::Debug for GDriveInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GDriveInstance")
    }
}

impl GDriveInstance {
    pub fn new(config: &Config, session_name: &str) -> Self {
        let fname = format!(
            "{}/{}_start_page_token",
            config.gdrive_token_path, session_name
        );
        let path = Path::new(&fname);
        Self {
            gdrive: Rc::new(Self::create_drive(&config, session_name).unwrap()),
            page_size: 1000,
            max_keys: None,
            session_name: session_name.to_string(),
            start_page_token: Self::read_start_page_token(&path).unwrap_or(None),
            start_page_token_filename: fname,
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

    pub fn with_start_page_token(mut self, start_page_token: &str) -> Self {
        self.start_page_token = Some(start_page_token.to_string());
        self
    }

    pub fn read_start_page_token_from_file(mut self) -> Self {
        let path = Path::new(&self.start_page_token_filename);
        self.start_page_token = Self::read_start_page_token(&path).unwrap_or(None);
        self
    }

    fn create_drive_auth(config: &Config, session_name: &str) -> Result<GCAuthenticator, Error> {
        let secret_file = File::open(&config.gdrive_secret_file)?;
        let secret: ConsoleApplicationSecret = serde_json::from_reader(secret_file)?;
        let secret = secret
            .installed
            .ok_or_else(|| err_msg("ConsoleApplicationSecret.installed is None"))?;
        let token_file = format!("{}/{}.json", config.gdrive_token_path, session_name);

        let parent = Path::new(&config.gdrive_token_path);

        if !parent.exists() {
            create_dir_all(parent)?;
        }

        let auth = Authenticator::new(
            &secret,
            DefaultAuthenticatorDelegate,
            Client::with_connector(HttpsConnector::new(NativeTlsClient::new()?)),
            DiskTokenStorage::new(&token_file)?,
            // Some(FlowType::InstalledInteractive),
            Some(FlowType::InstalledRedirect(8081)),
        );

        Ok(auth)
    }

    /// Creates a drive hub.
    fn create_drive(config: &Config, session_name: &str) -> Result<GCDrive, Error> {
        let auth = Self::create_drive_auth(config, session_name)?;
        Ok(Drive::new(
            Client::with_connector(HttpsConnector::new(NativeTlsClient::new()?)),
            auth,
        ))
    }

    fn get_filelist(
        &self,
        page_token: &Option<String>,
        get_folders: bool,
        parents: &Option<Vec<String>>,
    ) -> Result<drive3::FileList, Error> {
        let fields = vec![
            "name",
            "id",
            "size",
            "mimeType",
            "owners",
            "parents",
            "trashed",
            "modifiedTime",
            "createdTime",
            "viewedByMeTime",
            "md5Checksum",
            "fileExtension",
            "webContentLink",
        ];
        let fields = format!("nextPageToken,files({})", fields.join(","));
        exponential_retry(|| {
            let mut request = self
                .gdrive
                .files()
                .list()
                .param("fields", &fields)
                .spaces("drive") // TODO: maybe add photos as well
                .corpora("user")
                .page_size(self.page_size)
                .add_scope(drive3::Scope::Full);

            if let Some(token) = page_token {
                request = request.page_token(token);
            };

            let mut query_chain: Vec<String> = Vec::new();
            if get_folders {
                query_chain.push(r#"mimeType = "application/vnd.google-apps.folder""#.to_string());
            } else {
                query_chain.push(r#"mimeType != "application/vnd.google-apps.folder""#.to_string());
            }
            if let Some(ref p) = parents {
                let q = p
                    .iter()
                    .map(|id| format!("'{}' in parents", id))
                    .collect::<Vec<_>>()
                    .join(" or ");

                query_chain.push(format!("({})", q));
            }
            query_chain.push("trashed = false".to_string());

            let query = query_chain.join(" and ");
            let (_, filelist) = request
                .q(&query)
                .doit()
                .map_err(|e| format_err!("{:#?}", e))?;
            Ok(filelist)
        })
    }

    pub fn get_all_files(&self, get_folders: bool) -> Result<Vec<drive3::File>, Error> {
        let mut all_files = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let filelist = self.get_filelist(&page_token, get_folders, &None)?;

            if let Some(files) = filelist.files {
                all_files.extend(files);
            }

            page_token = filelist.next_page_token;
            if page_token.is_none() {
                break;
            }

            if let Some(max_keys) = self.max_keys {
                if all_files.len() > max_keys {
                    all_files.resize_with(max_keys, Default::default);
                    break;
                }
            }
        }
        Ok(all_files)
    }

    pub fn process_list_of_keys<T>(
        &self,
        parents: Option<Vec<String>>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&drive3::File) -> Result<(), Error>,
    {
        let mut n_processed = 0;
        let mut page_token: Option<String> = None;
        loop {
            let filelist = self.get_filelist(&page_token, false, &parents)?;

            if let Some(files) = filelist.files {
                for f in &files {
                    callback(f)?;
                    n_processed += 1;
                }
            }

            page_token = filelist.next_page_token;
            if page_token.is_none() {
                break;
            }

            if let Some(max_keys) = self.max_keys {
                if n_processed > max_keys {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn get_file_metadata(&self, id: &str) -> Result<drive3::File, Error> {
        self.gdrive
            .files()
            .get(id)
            .param("fields", "id,name,parents,mimeType,webContentLink")
            .add_scope(drive3::Scope::Full)
            .doit()
            .map(|(_response, file)| file)
            .map_err(|e| format_err!("{:#?}", e))
    }

    pub fn create_directory(&self, directory: &Url, parentid: &str) -> Result<(), Error> {
        let directory_path = directory
            .to_file_path()
            .map_err(|_| err_msg("No file path"))?;
        let directory_name = directory_path
            .file_name()
            .map(|d| d.to_string_lossy())
            .ok_or_else(|| err_msg("Failed to convert string"))?;
        exponential_retry(move || {
            let new_file = drive3::File {
                name: Some(directory_name.to_string()),
                mime_type: Some("application/vnd.google-apps.folder".to_string()),
                parents: Some(vec![parentid.to_string()]),
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
                .map_err(|e| format_err!("{:#?}", e))
                .map(|(_, _)| ())
        })
    }

    pub fn upload(&self, local: &Url, parentid: &str) -> Result<drive3::File, Error> {
        let file_path = local.to_file_path().map_err(|_| err_msg("No file path"))?;
        exponential_retry(|| {
            let file_obj = File::open(&file_path)?;
            let mime: Mime = "application/octet-stream"
                .parse()
                .map_err(|_| err_msg("bad mimetype"))?;
            let new_file = drive3::File {
                name: file_path
                    .as_path()
                    .file_name()
                    .and_then(OsStr::to_str)
                    .map(ToString::to_string),
                parents: Some(vec![parentid.to_string()]),
                ..Default::default()
            };

            self.gdrive
                .files()
                .create(new_file)
                .upload_resumable(file_obj, mime)
                .map_err(|e| format_err!("{:#?}", e))
                .map(|(_, f)| f)
        })
    }

    pub fn is_unexportable(&self, mime_type: &Option<String>) -> bool {
        if let Some(mime) = mime_type.clone() {
            UNEXPORTABLE_MIME_TYPES.contains::<str>(&mime)
        } else {
            false
        }
    }

    pub fn download(
        &self,
        gdriveid: &str,
        local: &Path,
        mime_type: &Option<String>,
    ) -> Result<(), Error> {
        if let Some(mime) = mime_type.clone() {
            if UNEXPORTABLE_MIME_TYPES.contains::<str>(&mime) {
                return Err(format_err!(
                    "UNEXPORTABLE_FILE: The MIME type of this \
                     file is {:?}, which can not be exported from Drive. Web \
                     content link provided by Drive: {:?}\n",
                    mime,
                    self.get_file_metadata(gdriveid)
                        .ok()
                        .map(|metadata| metadata.web_view_link)
                        .unwrap_or_default()
                ));
            }
        }

        let export_type: Option<&'static str> = mime_type
            .as_ref()
            .and_then(|ref t| MIME_TYPES.get::<str>(&t))
            .cloned();

        exponential_retry(move || {
            let mut response = match export_type {
                Some(t) => self
                    .gdrive
                    .files()
                    .export(gdriveid, &t)
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| format_err!("{:#?}", e))?,
                None => {
                    let (response, _empty_file) = self
                        .gdrive
                        .files()
                        .get(&gdriveid)
                        .supports_team_drives(false)
                        .param("alt", "media")
                        .add_scope(drive3::Scope::Full)
                        .doit()
                        .map_err(|e| format_err!("{:#?}", e))?;
                    response
                }
            };

            let mut content: Vec<u8> = Vec::new();
            response.read_to_end(&mut content)?;

            let mut outfile = File::create(local.to_path_buf())?;
            outfile.write_all(&content).map_err(err_msg)
        })
    }

    pub fn move_to_trash(&self, id: &str) -> Result<(), Error> {
        exponential_retry(|| {
            let mut f = drive3::File::default();
            f.trashed = Some(true);

            self.gdrive
                .files()
                .update(f, id)
                .add_scope(drive3::Scope::Full)
                .doit_without_upload()
                .map(|_| ())
                .map_err(|e| format_err!("DriveFacade::move_to_trash() {}", e))
        })
    }

    pub fn delete_permanently(&self, id: &str) -> Result<bool, Error> {
        exponential_retry(|| {
            self.gdrive
                .files()
                .delete(&id)
                .supports_team_drives(false)
                .add_scope(drive3::Scope::Full)
                .doit()
                .map(|response| response.status.is_success())
                .map_err(|e| format_err!("{:#?}", e))
        })
    }

    pub fn move_to(&self, id: &str, parent: &str, new_name: &str) -> Result<(), Error> {
        exponential_retry(|| {
            let current_parents = self
                .get_file_metadata(id)?
                .parents
                .unwrap_or_else(|| vec![String::from("root")])
                .join(",");

            let mut file = drive3::File::default();
            file.name = Some(new_name.to_string());
            self.gdrive
                .files()
                .update(file, id)
                .remove_parents(&current_parents)
                .add_parents(parent)
                .add_scope(drive3::Scope::Full)
                .doit_without_upload()
                .map_err(|e| format_err!("DriveFacade::move_to() {}", e))
                .map(|_| ())
        })
    }

    pub fn get_directory_map(
        &self,
    ) -> Result<(HashMap<String, DirectoryInfo>, Option<String>), Error> {
        let dlist: Vec<_> = self.get_all_files(true)?;
        let mut root_id: Option<String> = None;
        let mut dmap: HashMap<_, _> = dlist
            .iter()
            .filter_map(|d| {
                if let Some(owners) = d.owners.as_ref() {
                    if owners.is_empty() {
                        return None;
                    }
                    if owners[0].me != Some(true) {
                        return None;
                    }
                } else {
                    return None;
                }
                if let Some(gdriveid) = d.id.as_ref() {
                    if let Some(name) = d.name.as_ref() {
                        if let Some(parents) = d.parents.as_ref() {
                            if !parents.is_empty() {
                                return Some((
                                    gdriveid.to_string(),
                                    DirectoryInfo {
                                        directory_id: gdriveid.to_string(),
                                        directory_name: name.to_string(),
                                        parentid: Some(parents[0].to_string()),
                                    },
                                ));
                            }
                        } else {
                            if root_id.is_none()
                                && d.name != Some("Chrome Syncable FileSystem".to_string())
                            {
                                root_id = Some(gdriveid.to_string());
                            }
                            return Some((
                                gdriveid.to_string(),
                                DirectoryInfo {
                                    directory_id: gdriveid.to_string(),
                                    directory_name: name.to_string(),
                                    parentid: None,
                                },
                            ));
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
                    None => Some(p.to_string()),
                })
            })
            .collect();
        for parent in unmatched_parents {
            let d = self.get_file_metadata(&parent)?;
            if let Some(gdriveid) = d.id.as_ref() {
                if let Some(name) = d.name.as_ref() {
                    let parents = if let Some(p) = d.parents.as_ref() {
                        if !p.is_empty() {
                            Some(p[0].to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if parents.is_none()
                        && root_id.is_none()
                        && d.name != Some("Chrome Syncable FileSystem".to_string())
                    {
                        root_id = Some(gdriveid.to_string());
                    }
                    let val = DirectoryInfo {
                        directory_id: gdriveid.to_string(),
                        directory_name: name.to_string(),
                        parentid: parents,
                    };

                    dmap.entry(gdriveid.to_string()).or_insert(val);
                }
            }
        }
        Ok((dmap, root_id))
    }

    pub fn get_directory_name_map(
        directory_map: &HashMap<String, DirectoryInfo>,
    ) -> HashMap<String, Vec<DirectoryInfo>> {
        directory_map.values().fold(HashMap::new(), |mut h, m| {
            let key = m.directory_name.to_string();
            let val = m.clone();
            h.entry(key).or_insert_with(Vec::new).push(val);
            h
        })
    }

    pub fn get_export_path(
        &self,
        finfo: &drive3::File,
        dirmap: &HashMap<String, DirectoryInfo>,
    ) -> Result<Vec<String>, Error> {
        let mut fullpath = Vec::new();
        if let Some(name) = finfo.name.as_ref() {
            fullpath.push(name.to_string());
        }
        let mut pid = if let Some(parents) = finfo.parents.as_ref() {
            if !parents.is_empty() {
                Some(parents[0].to_string())
            } else {
                None
            }
        } else {
            None
        };
        loop {
            pid = if let Some(pid_) = pid.as_ref() {
                if let Some(dinfo) = dirmap.get(pid_) {
                    fullpath.push(format!("{}/", dinfo.directory_name));
                    dinfo.parentid.clone()
                } else {
                    self.get_file_metadata(pid_)
                        .ok()
                        .as_ref()
                        .and_then(|f| f.parents.as_ref())
                        .and_then(|v| {
                            if !v.is_empty() {
                                Some(v[0].to_string())
                            } else {
                                None
                            }
                        })
                }
            } else {
                None
            };
            if pid.is_none() {
                break;
            }
        }
        let fullpath: Vec<_> = fullpath.into_iter().rev().collect();
        Ok(fullpath)
    }

    pub fn get_parent_id(
        &self,
        url: &Url,
        dir_name_map: &HashMap<String, Vec<DirectoryInfo>>,
    ) -> Result<Option<String>, Error> {
        let mut previous_parent_id: Option<String> = None;
        if let Some(segments) = url.path_segments() {
            for seg in segments {
                let name = percent_decode(seg.as_bytes())
                    .decode_utf8_lossy()
                    .to_string();
                let mut matching_directory: Option<String> = None;
                if let Some(parents) = dir_name_map.get(&name) {
                    for parent in parents {
                        if previous_parent_id.is_none() {
                            previous_parent_id = Some(parent.directory_id.to_string());
                            matching_directory = Some(parent.directory_id.to_string());
                            break;
                        }
                        if parent.parentid.is_some() && parent.parentid == previous_parent_id {
                            matching_directory = Some(parent.directory_id.to_string())
                        }
                    }
                }
                if matching_directory.is_some() {
                    previous_parent_id = matching_directory.clone();
                } else {
                    return Ok(previous_parent_id);
                }
            }
        }
        Ok(None)
    }

    pub fn get_start_page_token(&self) -> Result<String, Error> {
        self.gdrive
            .changes()
            .get_start_page_token()
            .add_scope(drive3::Scope::Full)
            .doit()
            .map_err(|e| format_err!("{:#?}", e))
            .map(|result| {
                result.1.start_page_token.unwrap_or_else(|| {
                    err_msg(
                        "Received OK response from drive but there is no startPageToken included.",
                    )
                    .to_string()
                })
            })
    }

    pub fn store_start_page_token(&self, path: &Path) -> Result<(), Error> {
        if let Some(start_page_token) = self.start_page_token.as_ref() {
            let mut f = File::create(path)?;
            writeln!(f, "{}", start_page_token)?;
        }
        Ok(())
    }

    pub fn read_start_page_token(path: &Path) -> Result<Option<String>, Error> {
        if !path.exists() {
            return Ok(None);
        }
        let mut f = File::open(path)?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        Ok(Some(buf.trim().to_string()))
    }

    pub fn get_all_changes(&self) -> Result<Vec<drive3::Change>, Error> {
        if self.start_page_token.is_some() {
            let mut start_page_token = self.start_page_token.clone().unwrap();

            let mut all_changes = Vec::new();

            let changes_fields = vec!["kind", "type", "time", "removed", "fileId"];
            let file_fields = vec![
                "name",
                "id",
                "size",
                "mimeType",
                "owners",
                "parents",
                "trashed",
                "modifiedTime",
                "createdTime",
                "viewedByMeTime",
                "md5Checksum",
                "fileExtension",
                "webContentLink",
            ];
            let fields = format!(
                "kind,nextPageToken,newStartPageToken,changes({},file({}))",
                changes_fields.join(","),
                file_fields.join(",")
            );

            loop {
                let result = exponential_retry(|| {
                    self.gdrive
                        .changes()
                        .list(&start_page_token)
                        .param("fields", &fields)
                        .spaces("drive")
                        .restrict_to_my_drive(true)
                        .include_removed(true)
                        .supports_team_drives(false)
                        .include_team_drive_items(false)
                        .page_size(self.page_size)
                        .add_scope(drive3::Scope::Full)
                        .doit()
                        .map_err(|e| format_err!("{:#?}", e))
                });
                let (_response, changelist) = result?;

                match changelist.changes {
                    Some(changes) => all_changes.extend(changes),
                    _ => {
                        debug!("Changelist does not contain any changes!");
                        break;
                    }
                };
                if changelist.new_start_page_token.is_some() {
                    break;
                }
                match changelist.next_page_token {
                    Some(token) => start_page_token = token,
                    None => break,
                };
            }

            Ok(all_changes)
        } else {
            Ok(Vec::new())
        }
    }
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
    use std::collections::HashMap;
    use std::path::Path;
    use url::Url;

    use crate::config::Config;
    use crate::gdrive_instance::GDriveInstance;

    #[test]
    fn test_create_drive() {
        let config = Config::init_config().unwrap();
        let gdrive = GDriveInstance::new(&config, "ddboline@gmail.com")
            .with_max_keys(10)
            .with_page_size(10)
            .read_start_page_token_from_file();

        let list = gdrive.get_all_files(false).unwrap();
        assert_eq!(list.len(), 10);
        let test_info = list.iter().filter(|f| !f.parents.is_none()).nth(0).unwrap();
        println!("test_info {:?}", test_info);

        let gdriveid = test_info.id.as_ref().unwrap();
        let parent = &test_info.parents.as_ref().unwrap()[0];
        let local_path = Path::new("/tmp/temp.file");
        let mime = test_info.mime_type.as_ref().unwrap().to_string();
        println!("mime {}", mime);
        gdrive
            .download(&gdriveid, &local_path, &Some(mime))
            .unwrap();

        let basepath = Path::new("src/gdrive_instance.rs").canonicalize().unwrap();
        let local_url = Url::from_file_path(basepath).unwrap();
        let new_file = gdrive.upload(&local_url, &parent).unwrap();
        println!("new_file {:?}", new_file);
        println!("start_page_token {:?}", gdrive.start_page_token);
        println!(
            "current_start_page_token {:?}",
            gdrive.get_start_page_token().unwrap()
        );

        let changes = gdrive.get_all_changes().unwrap();
        let changes_map: HashMap<_, _> = changes
            .into_iter()
            .filter_map(|c| {
                if let Some((t, f)) = c
                    .file_id
                    .as_ref()
                    .and_then(|f| c.time.as_ref().and_then(|t| Some((f.clone(), t.clone()))))
                {
                    Some(((t, f), c))
                } else {
                    None
                }
            })
            .collect();
        println!("N files {}", changes_map.len());
        for ((f, t), ch) in changes_map {
            println!("ch {:?} {:?} {:?}", f, t, ch.file);
        }

        let new_driveid = new_file.id.unwrap();
        gdrive.move_to_trash(&new_driveid).unwrap();
        println!(
            "trash {:?}",
            gdrive.get_file_metadata(&new_driveid).unwrap()
        );

        gdrive.delete_permanently(&new_driveid).unwrap();
        println!(
            "error {}",
            gdrive.get_file_metadata(&new_driveid).unwrap_err()
        );
    }

    #[test]
    fn test_gdrive_store_read_change_token() {
        let config = Config::init_config().unwrap();
        let gdrive = GDriveInstance::new(&config, "ddboline@gmail.com")
            .with_max_keys(10)
            .with_page_size(10)
            .with_start_page_token("test_string");
        let p = Path::new("/tmp/temp_start_page_token.txt");
        gdrive.store_start_page_token(&p).unwrap();
        let result = GDriveInstance::read_start_page_token(&p).unwrap();
        assert_eq!(result, Some("test_string".to_string()));
    }
}