use google_drive3_fork as drive3;
use yup_oauth2 as oauth2;

use drive3::Drive;
use failure::{err_msg, Error};
use hyper::net::HttpsConnector;
use hyper::Client;
use hyper_native_tls::NativeTlsClient;
use oauth2::{
    Authenticator, ConsoleApplicationSecret, DefaultAuthenticatorDelegate, DiskTokenStorage,
    FlowType,
};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use url::Url;

use crate::config::Config;

type DriveId = String;
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
        println!("{}", config.gdrive_secret_file);
        let secret_file = File::open(config.gdrive_secret_file.clone())?;
        let secret: ConsoleApplicationSecret = serde_json::from_reader(secret_file)?;
        let secret = secret
            .installed
            .ok_or(err_msg("ConsoleApplicationSecret.installed is None"))?;
        println!("{}", config.gdrive_token_file);
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

    pub fn get_all_files(&self, parents: Option<Vec<DriveId>>) -> Result<Vec<drive3::File>, Error> {
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
                all_files.extend(files);
            }

            page_token = filelist.next_page_token;
            if page_token.is_none() {
                break;
            }
            println!("{}", all_files.len());

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

    pub fn upload(&self, local: &Url, remote: &Url) -> Result<(), Error> {
        Ok(())
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
                let mut response = self
                    .gdrive
                    .files()
                    .export(gdriveid, &t)
                    .add_scope(drive3::Scope::Full)
                    .doit()
                    .map_err(|e| err_msg(format!("{:#?}", e)))?;

                response
            }
            None => {
                let (mut response, _empty_file) = self
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
