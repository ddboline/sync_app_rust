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
use std::ffi::OsStr;
use std::fmt;
use std::fs::{create_dir_all, File};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::string::ToString;
use url::Url;

use crate::config::Config;

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
}

impl fmt::Debug for GDriveInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rc<GDriveInstance>")
    }
}

impl GDriveInstance {
    pub fn new(config: &Config, session_name: &str) -> Self {
        GDriveInstance {
            gdrive: Rc::new(GDriveInstance::create_drive(&config, session_name).unwrap()),
            page_size: 1000,
            max_keys: None,
            session_name: session_name.to_string(),
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

    fn create_drive_auth(config: &Config, session_name: &str) -> Result<GCAuthenticator, Error> {
        let secret_file = File::open(config.gdrive_secret_file.clone())?;
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
        println!("{}", query);
        let (_, filelist) = request
            .q(&query)
            .doit()
            .map_err(|e| err_msg(format!("{:#?}", e)))?;
        Ok(filelist)
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
        T: Fn(&drive3::File) -> (),
    {
        let mut n_processed = 0;
        let mut page_token: Option<String> = None;
        loop {
            let filelist = self.get_filelist(&page_token, false, &parents)?;

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

    fn get_file_metadata(&self, id: &str) -> Result<drive3::File, Error> {
        self.gdrive
            .files()
            .get(id)
            .param("fields", "id,name,parents,mimeType,webContentLink")
            .add_scope(drive3::Scope::Full)
            .doit()
            .map(|(_response, file)| file)
            .map_err(|e| err_msg(format!("{:#?}", e)))
    }

    pub fn create_directory(&self, directory: &Url, parentid: &str) -> Result<(), Error> {
        let directory_path = directory
            .to_file_path()
            .map_err(|_| err_msg("No file path"))?;
        let directory_name = directory_path
            .file_name()
            .and_then(|d| d.to_str().map(ToString::to_string))
            .ok_or_else(|| err_msg("Failed to convert string"))?;
        let new_file = drive3::File {
            name: Some(directory_name),
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
            .map_err(|e| err_msg(format!("{:#?}", e)))
            .map(|(_, _)| ())
    }

    pub fn upload(&self, local: &Url, parentid: &str) -> Result<drive3::File, Error> {
        let file_path = local.to_file_path().map_err(|_| err_msg("No file path"))?;
        let file_obj = File::open(file_path.clone())?;
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
            .map_err(|e| err_msg(format!("{:#?}", e)))
            .map(|(_, f)| f)
    }

    pub fn download(
        &self,
        gdriveid: &str,
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
            Some(t) => self
                .gdrive
                .files()
                .export(gdriveid, &t)
                .add_scope(drive3::Scope::Full)
                .doit()
                .map_err(|e| err_msg(format!("{:#?}", e)))?,
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

    pub fn move_to_trash(&self, id: &str) -> Result<(), Error> {
        let mut f = drive3::File::default();
        f.trashed = Some(true);

        self.gdrive
            .files()
            .update(f, id)
            .add_scope(drive3::Scope::Full)
            .doit_without_upload()
            .map(|_| ())
            .map_err(|e| err_msg(format!("DriveFacade::move_to_trash() {}", e)))
    }

    pub fn delete_permanently(&self, id: &str) -> Result<bool, Error> {
        self.gdrive
            .files()
            .delete(&id)
            .supports_team_drives(false)
            .add_scope(drive3::Scope::Full)
            .doit()
            .map(|response| response.status.is_success())
            .map_err(|e| err_msg(format!("{:#?}", e)))
    }

    pub fn move_to(&self, id: &str, parent: &str, new_name: &str) -> Result<(), Error> {
        let current_parents = self
            .get_file_metadata(id)?
            .parents
            .unwrap_or_else(|| vec![String::from("root")])
            .join(",");

        let mut file = drive3::File::default();
        file.name = Some(new_name.to_string());
        let _ = self
            .gdrive
            .files()
            .update(file, id)
            .remove_parents(&current_parents)
            .add_parents(parent)
            .add_scope(drive3::Scope::Full)
            .doit_without_upload()
            .map_err(|e| err_msg(format!("DriveFacade::move_to() {}", e)))?;
        Ok(())
    }

    pub fn get_directory_map(
        &self,
    ) -> Result<(HashMap<String, DirectoryInfo>, Option<String>), Error> {
        let dlist: Vec<_> = self.get_all_files(true)?;
        let mut dmap: HashMap<_, _> = dlist
            .iter()
            .filter_map(|d| {
                if let Some(gdriveid) = d.id.as_ref() {
                    if let Some(name) = d.name.as_ref() {
                        if let Some(parents) = d.parents.as_ref() {
                            if !parents.is_empty() {
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
        let mut root_id: Option<String> = None;
        for parent in unmatched_parents {
            let d = self.get_file_metadata(&parent)?;
            if let Some(gdriveid) = d.id.as_ref() {
                if let Some(name) = d.name.as_ref() {
                    let parents = if let Some(p) = d.parents.as_ref() {
                        if !p.is_empty() {
                            Some(p[0].clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    if parents.is_none() && root_id.is_none() {
                        root_id = parents.clone();
                    }
                    let val = DirectoryInfo {
                        gdriveid: gdriveid.clone(),
                        name: name.clone(),
                        parentid: parents,
                    };

                    dmap.entry(gdriveid.clone()).or_insert(val);
                }
            }
        }
        Ok((dmap, root_id))
    }

    pub fn get_export_path(
        &self,
        finfo: &drive3::File,
        dirmap: &HashMap<String, DirectoryInfo>,
    ) -> Result<Vec<String>, Error> {
        let mut fullpath = Vec::new();
        if let Some(name) = finfo.name.as_ref() {
            fullpath.push(format!("{}", name));
        }
        let mut pid = if let Some(parents) = finfo.parents.as_ref() {
            if !parents.is_empty() {
                Some(parents[0].clone())
            } else {
                None
            }
        } else {
            println!("No parents {:?}", finfo);
            None
        };
        loop {
            pid = if let Some(pid_) = pid.as_ref() {
                if let Some(dinfo) = dirmap.get(pid_) {
                    fullpath.push(format!("{}/", dinfo.name));
                    dinfo.parentid.clone()
                } else {
                    let pid_ = self
                        .get_file_metadata(pid_)
                        .ok()
                        .as_ref()
                        .and_then(|f| f.parents.as_ref())
                        .and_then(|v| {
                            if !v.is_empty() {
                                Some(v[0].clone())
                            } else {
                                None
                            }
                        });
                    println!("{:?}", pid_);
                    pid_
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
}

#[derive(Debug)]
pub struct DirectoryInfo {
    pub gdriveid: String,
    pub name: String,
    pub parentid: Option<String>,
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
    use std::path::Path;
    use url::Url;

    use crate::config::Config;
    use crate::gdrive_instance::GDriveInstance;

    #[test]
    fn test_create_drive() {
        let config = Config::new();
        let gdrive = GDriveInstance::new(&config, "ddboline@gmail.com")
            .with_max_keys(10)
            .with_page_size(10);
        let list = gdrive.get_all_files(false).unwrap();
        assert_eq!(list.len(), 10);
        let gdriveid = "1-vdOUMSwDYmMOQzyB1mpFiWGSBSCv_bJ9HRdCdj_yes".to_string();
        let parent = "0ABGM0lfCdptnUk9PVA".to_string();
        let local_url: Url = "file:///tmp/temp.file".parse().unwrap();
        let mime = "application/vnd.google-apps.spreadsheet".to_string();
        println!("{}", mime);
        gdrive.download(&gdriveid, &local_url, Some(mime)).unwrap();

        let basepath = Path::new("src/gdrive_instance.rs").canonicalize().unwrap();
        let local_url = Url::from_file_path(basepath).unwrap();
        let new_file = gdrive.upload(&local_url, &parent).unwrap();
        println!("{:?}", new_file);
        let new_driveid = new_file.id.unwrap();
        gdrive.move_to_trash(&new_driveid).unwrap();
        println!("{:?}", gdrive.get_file_metadata(&new_driveid).unwrap());
        gdrive.delete_permanently(&new_driveid).unwrap();
        println!("{}", gdrive.get_file_metadata(&new_driveid).unwrap_err());
    }
}
