use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use maplit::hashmap;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
    Client, Response, Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::{fs, task::spawn_blocking};

use crate::{config::Config, local_session::LocalSession, reqwest_session::ReqwestSession};

#[derive(Clone)]
pub struct SyncClient {
    pub session0: ReqwestSession,
    pub session1: ReqwestSession,
    pub local_session: LocalSession,
    pub config: Config,
}

impl SyncClient {
    pub fn new<T: AsRef<Path>>(config: Config, exe_path: T) -> Self {
        Self {
            session0: ReqwestSession::new(true),
            session1: ReqwestSession::new(true),
            local_session: LocalSession::new(exe_path),
            config,
        }
    }

    pub fn get_url(&self) -> Result<Url, Error> {
        let from_url: Url = self
            .config
            .garmin_from_url
            .as_ref()
            .ok_or_else(|| format_err!("No From URL"))?
            .clone();
        Ok(from_url)
    }

    pub async fn init(&self, base_url: &str) -> Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct LoggedUser {
            email: String,
        }

        let from_url = self.get_url()?;
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

        Ok(())
    }

    pub async fn get_remote<T: DeserializeOwned>(&self, url: &Url) -> Result<Vec<T>, Error> {
        let resp = self.session0.get(&url, &HeaderMap::new()).await?;
        resp.json().await.map_err(Into::into)
    }

    pub async fn put_remote<T: Serialize>(
        &self,
        url: &Url,
        data: &[T],
        js_prefix: &str,
    ) -> Result<(), Error> {
        if !data.is_empty() {
            for data in data.chunks(10) {
                let data = hashmap! {
                    js_prefix => data,
                };
                self.session0
                    .post(&url, &HeaderMap::new(), &data)
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(())
    }

    pub async fn get_local<T: DeserializeOwned + Send + 'static>(
        &self,
        table: &str,
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<Vec<T>, Error> {
        let f = NamedTempFile::new()?;
        self.local_session
            .export(table, &f, start_timestamp)
            .await?;
        let data = fs::read(f).await?;
        spawn_blocking(move || serde_json::from_slice(&data).map_err(Into::into)).await?
    }

    pub async fn put_local<T: Serialize>(
        &self,
        table: &str,
        data: &[T],
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let f = NamedTempFile::new()?;
        let data = serde_json::to_vec(&data)?;
        fs::write(&f, data).await?;
        self.local_session
            .import(table, &f, start_timestamp)
            .await?;
        Ok(())
    }
}
