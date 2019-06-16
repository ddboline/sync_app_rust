use failure::Error;
use onedrive_api::{AuthClient, DriveClient, DriveLocation, Permission};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

use crate::config::Config;
use crate::map_result_vec;

pub struct OneDriveInstance {
    pub onedrive: Arc<DriveClient>,
    pub session_name: String,
}

#[derive(Default, Clone)]
pub struct OneDriveCredentials {
    pub redirect_uri: String,
    pub client_id: String,
    pub client_secret: String,
}

impl OneDriveCredentials {
    pub fn read_from_file(filename: &str) -> Result<OneDriveCredentials, Error> {
        let f = File::open(filename)?;
        let b = BufReader::new(f);
        let mut credentials: OneDriveCredentials = Default::default();
        for l in b.lines() {
            let line = l?;
            let entries: Vec<_> = line.split_whitespace().take(2).collect();
            if entries.len() == 2 {
                match entries[0] {
                    "redirect_uri" => credentials.redirect_uri = entries[1].to_string(),
                    "client_id" => credentials.client_id = entries[1].to_string(),
                    "client_secret" => credentials.client_secret = entries[1].to_string(),
                    _ => (),
                };
            }
        }
        Ok(credentials)
    }
}

impl OneDriveInstance {
    pub fn new(config: &Config, session_name: &str) -> Self {
        Self {
            onedrive: Arc::new(OneDriveInstance::create_drive(config, session_name).unwrap()),
            session_name: session_name.to_string(),
        }
    }

    pub fn create_drive(config: &Config, session_name: &str) -> Result<DriveClient, Error> {
        let perm = Permission::new_read()
            .write(true)
            .access_shared(true)
            .offline_access(true);
        println!("{}", config.onedrive_credential_file);
        let cred = OneDriveCredentials::read_from_file(&config.onedrive_credential_file)?;
        let auth = AuthClient::new(cred.client_id.clone(), perm, cred.redirect_uri.clone());

        let auth_url = auth.get_token_auth_url();
        println!("{} {}", auth_url, cred.redirect_uri);
        panic!("{}", auth_url);
        let code = "".to_string();
        let token = auth.login_with_code(&code, None)?;
        Ok(DriveClient::new(token.token, DriveLocation::me()))
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::onedrive_instance::{OneDriveInstance, OneDriveCredentials};

    #[test]
    fn test_onedrive_read_credentials() {
        let cred = OneDriveCredentials::read_from_file("tests/data/onedrive.credentials").unwrap();
        assert_eq!(cred.redirect_uri, "http://localhost:8080/");
        assert_eq!(cred.client_id, "ABCDEFG");
        assert_eq!(cred.client_secret, "abcdefg012345");
    }

    #[test]
    fn test_onedrive_create_drive() {
        let config = Config::init_config().unwrap();
        let session_name = "test_session";
        let client = OneDriveInstance::create_drive(&config, session_name).unwrap();

        assert!(false);
    }
}
