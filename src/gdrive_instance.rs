use drive3::Drive;
use failure::{err_msg, Error};
use google_drive3_fork as drive3;
use hyper::net::HttpsConnector;
use hyper::Client;
use hyper_native_tls::NativeTlsClient;
use oauth2::{
    Authenticator, ConsoleApplicationSecret, DefaultAuthenticatorDelegate, DiskTokenStorage,
    FlowType,
};
use std::fs::File;
use std::sync::Arc;
use yup_oauth2 as oauth2;

use crate::config::Config;

type DriveId = String;
type GCClient = Client;
type GCAuthenticator = Authenticator<DefaultAuthenticatorDelegate, DiskTokenStorage, Client>;
type GCDrive = Drive<GCClient, GCAuthenticator>;

pub struct GDriveInstance {
    pub gdrive: Arc<GCDrive>,
    pub page_size: i32,
    pub max_keys: Option<usize>,
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
        return Ok(all_files);
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
