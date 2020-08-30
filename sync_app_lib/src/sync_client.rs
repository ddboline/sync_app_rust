use reqwest::Url;

use crate::{config::Config, local_session::LocalSession, reqwest_session::RequestSession};

#[derive(Clone)]
pub struct SyncClient {
    pub session0: ReqwestSession,
    pub session1: ReqwestSession,
    pub local_session: LocalSession,
    pub config: Config,
}

impl SyncClient {
    pub fn new(config: Config) -> Self {
        Self {
            session0: ReqwestSession::new(true),
            session1: ReqwestSession::new(true),
            local_session: LocalSession {
                exe_path: "/tmp".into(),
            },
            config,
        }
    }

    pub fn get_urls(&self) -> Result<(Url, Url), Error> {
        let from_url: Url = self
            .config
            .garmin_from_url
            .as_ref()
            .ok_or_else(|| format_err!("No From URL"))?
            .clone();
        let to_url: Url = self
            .config
            .garmin_to_url
            .as_ref()
            .ok_or_else(|| format_err!("No To URL"))?
            .clone();
        Ok((from_url, to_url))
    }

    pub async fn init(&self, base_url: &str) -> Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct LoggedUser {
            email: String,
        }

        let (from_url, to_url) = self.get_urls()?;
        let user = self
            .config
            .garmin_username
            .as_ref()
            .ok_or_else(|| format_err!("No Username"))?;
        let password = self
            .config
            .garmin_password
            .as_ref()
            .ok_or_else(|| format_err!("No Password"))?;

        let data = hashmap! {
            "email" => user.as_str(),
            "password" => password.as_str(),
        };

        let url = from_url.join("api/auth")?;
        let resp = self
            .session0
            .post(&url, &HeaderMap::new(), &data)
            .await?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();

        let url = to_url.join("api/auth")?;
        let resp = self
            .session1
            .post(&url, &HeaderMap::new(), &data)
            .await?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();

        let url = from_url.join(&format!("{}/user", base_url))?;
        let resp = self
            .session0
            .get(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        let _: LoggedUser = resp
            .json()
            .await
            .map_err(|e| format_err!("Login problem {:?}", e))?;

        let url = to_url.join(&format!("{}/user", base_url))?;
        let resp = self
            .session1
            .get(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        let _: LoggedUser = resp
            .json()
            .await
            .map_err(|e| format_err!("Login problem {:?}", e))?;

        Ok(())
    }
}
