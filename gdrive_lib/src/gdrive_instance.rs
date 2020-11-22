use anyhow::{format_err, Error};
use chrono::DateTime;
use drive3::Drive;
use google_drive3_fork as drive3;
use hyper::{net::HttpsConnector, Client};
use hyper_native_tls::NativeTlsClient;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::debug;
use maplit::{hashmap, hashset};
use mime::Mime;
use oauth2::{
    Authenticator, ConsoleApplicationSecret, DefaultAuthenticatorDelegate, DiskTokenStorage,
    FlowType,
};
use parking_lot::Mutex;
use percent_encoding::percent_decode;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use stack_string::StackString;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fmt::{self, Debug, Formatter},
    fs::{self, create_dir_all, File},
    io,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    string::ToString,
    sync::Arc,
};
use url::Url;
use yup_oauth2 as oauth2;

use crate::{directory_info::DirectoryInfo, exponential_retry};

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
    pub gdrive: Arc<Mutex<GCDrive>>,
    pub page_size: i32,
    pub max_keys: Option<usize>,
    pub session_name: StackString,
    pub start_page_token: Option<usize>,
    pub start_page_token_filename: PathBuf,
}

impl Debug for GDriveInstance {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "GDriveInstance")
    }
}

impl GDriveInstance {
    pub fn new(gdrive_token_path: &Path, gdrive_secret_file: &Path, session_name: &str) -> Self {
        let fname = gdrive_token_path.join(format!("{}_start_page_token", session_name));

        Self {
            gdrive: Arc::new(Mutex::new(
                Self::create_drive(gdrive_token_path, gdrive_secret_file, session_name).unwrap(),
            )),
            page_size: 1000,
            max_keys: None,
            session_name: session_name.into(),
            start_page_token: Self::read_start_page_token(&fname).unwrap_or(None),
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

    pub fn with_start_page_token(mut self, start_page_token: usize) -> Self {
        self.start_page_token = Some(start_page_token);
        self
    }

    pub fn read_start_page_token_from_file(mut self) -> Self {
        self.start_page_token =
            Self::read_start_page_token(&self.start_page_token_filename).unwrap_or(None);
        self
    }

    fn create_drive_auth(
        gdrive_token_path: &Path,
        gdrive_secret_file: &Path,
        session_name: &str,
    ) -> Result<GCAuthenticator, Error> {
        let secret_file = File::open(gdrive_secret_file)?;
        let secret: ConsoleApplicationSecret = serde_json::from_reader(secret_file)?;
        let secret = secret
            .installed
            .ok_or_else(|| format_err!("ConsoleApplicationSecret.installed is None"))?;
        let token_file = gdrive_token_path.join(format!("{}.json", session_name));
        let token_file = token_file.to_string_lossy().into_owned();

        if !gdrive_token_path.exists() {
            create_dir_all(gdrive_token_path)?;
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
    fn create_drive(
        gdrive_token_path: &Path,
        gdrive_secret_file: &Path,
        session_name: &str,
    ) -> Result<GCDrive, Error> {
        let auth = Self::create_drive_auth(gdrive_token_path, gdrive_secret_file, session_name)?;
        Ok(Drive::new(
            Client::with_connector(HttpsConnector::new(NativeTlsClient::new()?)),
            auth,
        ))
    }

    fn get_filelist(
        &self,
        page_token: &Option<StackString>,
        get_folders: bool,
        parents: &Option<Vec<StackString>>,
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
            let gdrive = self.gdrive.lock();
            let mut request = gdrive
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

            let mut query_chain: Vec<StackString> = Vec::new();
            if get_folders {
                query_chain.push(r#"mimeType = "application/vnd.google-apps.folder""#.into());
            } else {
                query_chain.push(r#"mimeType != "application/vnd.google-apps.folder""#.into());
            }
            if let Some(ref p) = parents {
                let q = p
                    .iter()
                    .map(|id| format!("'{}' in parents", id))
                    .join(" or ");

                query_chain.push(format!("({})", q).into());
            }
            query_chain.push("trashed = false".into());

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
        let mut page_token: Option<StackString> = None;
        loop {
            let filelist = self.get_filelist(&page_token, get_folders, &None)?;

            if let Some(files) = filelist.files {
                all_files.extend(files);
            }

            page_token = filelist.next_page_token.map(Into::into);
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

    pub fn get_all_file_info(
        &self,
        get_folders: bool,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<GDriveInfo>, Error> {
        let files = self.get_all_files(get_folders)?;
        self.convert_file_list_to_gdrive_info(&files, directory_map)
    }

    pub fn convert_file_list_to_gdrive_info(
        &self,
        flist: &[drive3::File],
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<GDriveInfo>, Error> {
        flist
            .par_iter()
            .filter(|f| {
                if let Some(owners) = f.owners.as_ref() {
                    if owners.is_empty() {
                        return false;
                    }
                    if owners[0].me != Some(true) {
                        return false;
                    }
                } else {
                    return false;
                }
                if Self::is_unexportable(&f.mime_type) {
                    return false;
                }
                true
            })
            .map(|f| GDriveInfo::from_object(f, &self, directory_map))
            .collect()
    }

    pub fn process_list_of_keys<T>(
        &self,
        parents: &Option<Vec<StackString>>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&drive3::File) -> Result<(), Error>,
    {
        let mut n_processed = 0;
        let mut page_token: Option<StackString> = None;
        loop {
            let filelist = self.get_filelist(&page_token, false, parents)?;

            if let Some(files) = filelist.files {
                for f in &files {
                    callback(f)?;
                    n_processed += 1;
                }
            }

            page_token = filelist.next_page_token.map(Into::into);
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
            .lock()
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
            .map_err(|_| format_err!("No file path"))?;
        let directory_name = directory_path
            .file_name()
            .map(OsStr::to_string_lossy)
            .ok_or_else(|| format_err!("Failed to convert string"))?;
        exponential_retry(move || {
            let new_file = drive3::File {
                name: Some(directory_name.to_string()),
                mime_type: Some("application/vnd.google-apps.folder".to_string()),
                parents: Some(vec![parentid.to_string()]),
                ..drive3::File::default()
            };
            let mime: Mime = "application/octet-stream"
                .parse()
                .map_err(|_| format_err!("bad mimetype"))?;
            let dummy_file = DummyFile::new(&[]);

            self.gdrive
                .lock()
                .files()
                .create(new_file)
                .upload(dummy_file, mime)
                .map_err(|e| format_err!("{:#?}", e))
                .map(|(_, _)| ())
        })
    }

    pub fn upload(&self, local: &Url, parentid: &str) -> Result<drive3::File, Error> {
        let file_path = local
            .to_file_path()
            .map_err(|_| format_err!("No file path"))?;
        exponential_retry(|| {
            let file_obj = File::open(&file_path)?;
            let mime: Mime = "application/octet-stream"
                .parse()
                .map_err(|_| format_err!("bad mimetype"))?;
            let new_file = drive3::File {
                name: file_path
                    .as_path()
                    .file_name()
                    .and_then(OsStr::to_str)
                    .map(ToString::to_string),
                parents: Some(vec![parentid.to_string()]),
                ..drive3::File::default()
            };

            self.gdrive
                .lock()
                .files()
                .create(new_file)
                .upload_resumable(file_obj, mime)
                .map_err(|e| format_err!("{:#?}", e))
                .map(|(_, f)| f)
        })
    }

    pub fn is_unexportable<T: AsRef<str>>(mime_type: &Option<T>) -> bool {
        mime_type.as_ref().map_or(false, |mime| {
            UNEXPORTABLE_MIME_TYPES.contains::<str>(mime.as_ref())
        })
    }

    pub fn export(&self, gdriveid: &str, local: &Path, mime_type: &str) -> Result<(), Error> {
        exponential_retry(move || {
            let mut content = Vec::new();
            self.gdrive
                .lock()
                .files()
                .export(gdriveid, mime_type)
                .add_scope(drive3::Scope::Full)
                .doit()
                .map_err(|e| format_err!("{:#?}", e))?
                .read_to_end(&mut content)?;

            fs::write(local, &content).map_err(Into::into)
        })
    }

    pub fn download<T>(
        &self,
        gdriveid: &str,
        local: &Path,
        mime_type: &Option<T>,
    ) -> Result<(), Error>
    where
        T: AsRef<str> + Debug,
    {
        if let Some(mime) = mime_type {
            if UNEXPORTABLE_MIME_TYPES.contains::<str>(mime.as_ref()) {
                return Err(format_err!(
                    "UNEXPORTABLE_FILE: The MIME type of this file is {:?}, which can not be \
                     exported from Drive. Web content link provided by Drive: {:?}\n",
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
            .and_then(|t| MIME_TYPES.get::<str>(t.as_ref()))
            .cloned();

        exponential_retry(move || {
            let mut response = if let Some(t) = export_type {
                self.gdrive
                    .lock()
                    .files()
                    .export(gdriveid, &t)
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| format_err!("{:#?}", e))?
            } else {
                let (response, _empty_file) = self
                    .gdrive
                    .lock()
                    .files()
                    .get(&gdriveid)
                    .supports_team_drives(false)
                    .param("alt", "media")
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| format_err!("{:#?}", e))?;
                response
            };

            let mut content = Vec::new();
            response.read_to_end(&mut content)?;

            fs::write(local, &content).map_err(Into::into)
        })
    }

    pub fn move_to_trash(&self, id: &str) -> Result<(), Error> {
        exponential_retry(|| {
            let mut f = drive3::File::default();
            f.trashed = Some(true);

            self.gdrive
                .lock()
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
                .lock()
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
                .lock()
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
    ) -> Result<(HashMap<StackString, DirectoryInfo>, Option<StackString>), Error> {
        let mut root_id: Option<StackString> = None;
        let mut dmap: HashMap<StackString, _> = self
            .get_all_files(true)?
            .into_iter()
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
                                    gdriveid.into(),
                                    DirectoryInfo {
                                        directory_id: gdriveid.into(),
                                        directory_name: name.into(),
                                        parentid: Some(parents[0].clone().into()),
                                    },
                                ));
                            }
                        } else {
                            if root_id.is_none()
                                && d.name != Some("Chrome Syncable FileSystem".to_string())
                            {
                                root_id = Some(gdriveid.into());
                            }
                            return Some((
                                gdriveid.into(),
                                DirectoryInfo {
                                    directory_id: gdriveid.into(),
                                    directory_name: name.into(),
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
                    let parents = d
                        .parents
                        .as_ref()
                        .and_then(|p| p.get(0).map(ToString::to_string));
                    if parents.is_none()
                        && root_id.is_none()
                        && d.name != Some("Chrome Syncable FileSystem".to_string())
                    {
                        root_id = Some(gdriveid.into());
                    }
                    let val = DirectoryInfo {
                        directory_id: gdriveid.into(),
                        directory_name: name.into(),
                        parentid: parents.map(Into::into),
                    };

                    dmap.entry(gdriveid.into()).or_insert(val);
                }
            }
        }
        Ok((dmap, root_id))
    }

    pub fn get_directory_name_map(
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> HashMap<StackString, Vec<DirectoryInfo>> {
        directory_map.values().fold(HashMap::new(), |mut h, m| {
            let key = m.directory_name.clone();
            let val = m.clone();
            h.entry(key).or_insert_with(Vec::new).push(val);
            h
        })
    }

    pub fn get_export_path(
        &self,
        finfo: &drive3::File,
        dirmap: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<StackString>, Error> {
        let mut fullpath = Vec::new();
        if let Some(name) = finfo.name.as_ref() {
            fullpath.push(name.clone().into());
        }
        let mut pid: Option<StackString> = finfo
            .parents
            .as_ref()
            .and_then(|parents| parents.get(0).map(|p| p.to_string().into()));
        loop {
            pid = if let Some(pid_) = pid.as_ref() {
                if let Some(dinfo) = dirmap.get(pid_) {
                    fullpath.push(format!("{}/", dinfo.directory_name).into());
                    dinfo.parentid.clone()
                } else {
                    self.get_file_metadata(pid_)
                        .ok()
                        .as_ref()
                        .and_then(|f| f.parents.as_ref())
                        .and_then(|v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v[0].to_string().into())
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
        Ok(fullpath.into_iter().rev().collect())
    }

    pub fn get_parent_id(
        url: &Url,
        dir_name_map: &HashMap<StackString, Vec<DirectoryInfo>>,
    ) -> Result<Option<StackString>, Error> {
        let mut previous_parent_id: Option<StackString> = None;
        if let Some(segments) = url.path_segments() {
            for seg in segments {
                let name = percent_decode(seg.as_bytes())
                    .decode_utf8_lossy()
                    .to_string();
                let mut matching_directory: Option<StackString> = None;
                if let Some(parents) = dir_name_map.get(name.as_str()) {
                    for parent in parents {
                        if previous_parent_id.is_none() {
                            previous_parent_id = Some(parent.directory_id.clone());
                            matching_directory = Some(parent.directory_id.clone());
                            break;
                        }
                        if parent.parentid.is_some() && parent.parentid == previous_parent_id {
                            matching_directory = Some(parent.directory_id.clone())
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

    pub fn get_start_page_token(&self) -> Result<usize, Error> {
        self.gdrive
            .lock()
            .changes()
            .get_start_page_token()
            .add_scope(drive3::Scope::Full)
            .doit()
            .map_err(|e| format_err!("{:#?}", e))
            .and_then(|(_, token)| {
                token
                    .start_page_token
                    .ok_or_else(|| {
                        format_err!(
                            "Received OK response from drive but there is no startPageToken \
                             included.",
                        )
                    })
                    .and_then(|s| s.parse().map_err(Into::into))
            })
    }

    pub fn store_start_page_token(&self, path: &Path) -> Result<(), Error> {
        if let Some(start_page_token) = self.start_page_token.as_ref() {
            let mut f = File::create(path)?;
            write!(f, "{}", start_page_token)?;
        }
        Ok(())
    }

    pub fn read_start_page_token(path: &Path) -> Result<Option<usize>, Error> {
        if !path.exists() {
            return Ok(None);
        }
        let mut f = File::open(path)?;
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        let start_page_token = buf.parse()?;
        Ok(Some(start_page_token))
    }

    pub fn get_all_changes(&self) -> Result<Vec<drive3::Change>, Error> {
        if self.start_page_token.is_some() {
            let mut start_page_token = self.start_page_token.as_ref().unwrap().to_string();

            let mut all_changes = Vec::new();

            let changes_fields = ["kind", "type", "time", "removed", "fileId"].join(",");
            let file_fields = [
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
            ]
            .join(",");
            let fields = format!(
                "kind,nextPageToken,newStartPageToken,changes({},file({}))",
                changes_fields, file_fields,
            );

            loop {
                let result = exponential_retry(|| {
                    self.gdrive
                        .lock()
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

                if let Some(changes) = changelist.changes {
                    all_changes.extend(changes);
                } else {
                    debug!("Changelist does not contain any changes!");
                    break;
                }
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

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct GDriveInfo {
    pub filename: StackString,
    pub filepath: Option<PathBuf>,
    pub urlname: Option<Url>,
    pub md5sum: Option<StackString>,
    pub sha1sum: Option<StackString>,
    pub filestat: Option<(u32, u32)>,
    pub serviceid: Option<StackString>,
    pub servicesession: Option<StackString>,
}

impl GDriveInfo {
    pub fn from_object(
        item: &drive3::File,
        gdrive: &GDriveInstance,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Self, Error> {
        let filename = item
            .name
            .as_ref()
            .ok_or_else(|| format_err!("No filename"))?;
        let md5sum = item.md5_checksum.as_ref().and_then(|x| x.parse().ok());
        let st_mtime = DateTime::parse_from_rfc3339(
            item.modified_time
                .as_ref()
                .ok_or_else(|| format_err!("No last modified"))?,
        )?
        .timestamp();
        let size: u32 = item.size.as_ref().and_then(|x| x.parse().ok()).unwrap_or(0);
        let serviceid = item.id.as_ref().map(Into::into);
        let servicesession = Some(gdrive.session_name.parse()?);

        let export_path = gdrive.get_export_path(&item, &directory_map)?;
        let filepath = export_path.iter().fold(PathBuf::new(), |mut p, e| {
            p.push(e.as_str());
            p
        });
        let urlname = format!("gdrive://{}/", gdrive.session_name);
        let urlname = Url::parse(&urlname)?;
        let urlname = export_path.iter().fold(urlname, |u, e| {
            if e.contains('#') {
                u.join(&e.replace("#", "%35")).unwrap()
            } else {
                u.join(e).unwrap()
            }
        });

        let finfo = Self {
            filename: filename.into(),
            filepath: Some(filepath),
            urlname: Some(urlname),
            md5sum,
            sha1sum: None,
            filestat: Some((st_mtime as u32, size as u32)),
            serviceid,
            servicesession,
        };
        if item.id == Some("1t4plcsKgXK_NB025K01yFLKwljaTeM3i".to_string()) {
            debug!("{:?}, {:?}", item, finfo);
        }

        Ok(finfo)
    }

    pub fn from_changes_object(
        item: drive3::Change,
        gdrive: &GDriveInstance,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Self, Error> {
        let file = item.file.ok_or_else(|| format_err!("No file"))?;
        Self::from_object(&file, gdrive, directory_map)
    }
}

struct DummyFile {
    cursor: u64,
    data: Vec<u8>,
}

impl DummyFile {
    fn new(data: &[u8]) -> Self {
        Self {
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
