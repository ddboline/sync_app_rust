use failure::Error;
use onedrive_api::{AuthClient, DriveClient, DriveLocation, Permission};
use std::sync::Arc;

use crate::config::Config;

pub struct OneDriveInstance {
    pub onedrive: Arc<DriveClient>,
    pub session_name: String,
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
        let auth = AuthClient::new(
            config.onedrive_client_id.clone(),
            perm,
            config.onedrive_redirect_uri.clone(),
        );

        let auth_url = auth.get_code_auth_url();
        let code = "".to_string();
        let token = auth.login_with_code(&code, None)?;
        Ok(DriveClient::new(token.token, DriveLocation::me()))
    }
}
