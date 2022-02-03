use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use log::debug;
use maplit::hashmap;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
    Client, Response, Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{fmt::Write, path::Path};
use tokio::{fs, task::spawn_blocking};

use crate::{config::Config, local_session::LocalSession, reqwest_session::ReqwestSession};

#[derive(Clone)]
pub struct SyncClient {
    remote_session: ReqwestSession,
    local_session: LocalSession,
    config: Config,
}

impl SyncClient {
    pub fn new<T: AsRef<Path>>(config: Config, exe_path: T) -> Self {
        Self {
            remote_session: ReqwestSession::new(true),
            local_session: LocalSession::new(exe_path),
            config,
        }
    }

    pub fn get_url(&self) -> Result<Url, Error> {
        let from_url: Url = self
            .config
            .remote_url
            .as_ref()
            .ok_or_else(|| format_err!("No From URL"))?
            .clone()
            .into();
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
            .remote_username
            .as_ref()
            .ok_or_else(|| format_err!("No Username"))?;
        let password = self
            .config
            .remote_password
            .as_ref()
            .ok_or_else(|| format_err!("No Password"))?;

        let data = hashmap! {
            "email" => user.as_str(),
            "password" => password.as_str(),
        };

        let url = from_url.join("api/auth")?;
        self.remote_session
            .post(&url, &HeaderMap::new(), &data)
            .await?
            .error_for_status()?;
        let buf = format_sstr!("{base_url}/user");
        let url = from_url.join(&buf)?;
        let resp = self
            .remote_session
            .get(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        let user: LoggedUser = resp
            .json()
            .await
            .map_err(|e| format_err!("Login problem {e:?}"))?;
        debug!("user: {:?}", user.email);
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let from_url = self.get_url()?;

        let url = from_url.join("api/auth")?;
        self.remote_session
            .delete(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn get_remote<T: DeserializeOwned>(&self, url: &Url) -> Result<Vec<T>, Error> {
        let resp = self.remote_session.get(url, &HeaderMap::new()).await?;
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
                self.remote_session
                    .post(url, &HeaderMap::new(), &data)
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
        let data = self
            .local_session
            .run_command_export(table, start_timestamp)
            .await?;
        if data.is_empty() {
            Ok(Vec::new())
        } else {
            spawn_blocking(move || serde_json::from_slice(&data).map_err(Into::into)).await?
        }
    }

    pub async fn get_local_command<T: DeserializeOwned + Send + 'static>(
        &self,
        args: &[&str],
    ) -> Result<Vec<T>, Error> {
        let data = self.local_session.run_command(args).await?;
        if data.is_empty() {
            Ok(Vec::new())
        } else {
            spawn_blocking(move || serde_json::from_slice(&data).map_err(Into::into)).await?
        }
    }

    pub async fn put_local<T: Serialize>(
        &self,
        table: &str,
        data: &[T],
        start_timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let data = serde_json::to_vec(&data)?;
        self.local_session
            .run_command_import(table, &data, start_timestamp)
            .await
    }
}
