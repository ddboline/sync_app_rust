use anyhow::{format_err, Error};
use log::debug;
use maplit::hashmap;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{path::Path, time::Duration};
use time::OffsetDateTime;
use tokio::{task::spawn_blocking, time::timeout};
use uuid::Uuid;

use crate::{config::Config, local_session::LocalSession, reqwest_session::ReqwestSession};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct Pagination {
    pub limit: usize,
    pub offset: usize,
    pub total: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Paginated<T> {
    pub pagination: Pagination,
    pub data: Vec<T>,
}

#[derive(Clone)]
pub struct SyncClient {
    remote_session: ReqwestSession,
    local_session: LocalSession,
    config: Config,
}

impl SyncClient {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new<T: AsRef<Path>>(config: Config, exe_path: T) -> Result<Self, Error> {
        Ok(Self {
            remote_session: ReqwestSession::new(true)?,
            local_session: LocalSession::new(exe_path),
            config,
        })
    }

    /// # Errors
    /// Return error if api call fails
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

    /// # Errors
    /// Return error if api call fails
    pub async fn init(&self, base_url: &str, label: impl Into<Option<&str>>) -> Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct LoggedUser {
            email: String,
            session: Uuid,
            secret_key: StackString,
        }

        let from_url = self.get_url()?;

        let url = from_url.join("api/status")?;
        // If status endpoint doesn't return after 60 seconds we should exit.
        timeout(
            Duration::from_secs(60),
            self.remote_session.get(&url, &HeaderMap::new()),
        )
        .await??
        .error_for_status()?;

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

        if let Some(label) = label.into() {
            let url = from_url.join(format_sstr!("api/session/{label}").as_str())?;
            let session_str = format_sstr!("{}", user.session);
            let value = HeaderValue::from_str(&session_str)?;
            let secret_key = HeaderValue::from_str(&user.secret_key)?;
            let data = hashmap! {
                "label" => label,
                "status" => "running"
            };
            let mut headermap = HeaderMap::new();
            headermap.append("session", value);
            headermap.append("secret-key", secret_key);
            self.remote_session
                .post(&url, &headermap, &data)
                .await?
                .error_for_status()?;
        }

        debug!("user: {:?}", user.email);
        Ok(())
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn shutdown(&self) -> Result<(), Error> {
        let from_url = self.get_url()?;

        let url = from_url.join("api/auth")?;
        self.remote_session
            .delete(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_remote<T: DeserializeOwned>(&self, url: &Url) -> Result<Vec<T>, Error> {
        let resp = self
            .remote_session
            .get(url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        resp.json().await.map_err(Into::into)
    }

    async fn _get_remote_paginated<T: DeserializeOwned>(&self, url: &Url, offset: usize, limit: usize) -> Result<Paginated<T>, Error> {
        let offset = format_sstr!("{offset}");
        let limit = format_sstr!("{limit}");
        let options = [("offset", &offset), ("limit", &limit)];
        let url = Url::parse_with_params(url.as_str(), &options)?;
        let resp = self
            .remote_session
            .get(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        resp.json().await.map_err(Into::into)
    }

    pub async fn get_remote_paginated<T: DeserializeOwned>(&self, url: &Url) -> Result<Vec<T>, Error> {
        let mut result = Vec::new();
        let mut offset = 0;
        let limit = 10;
        let mut total = None;
        loop {
            let mut response = self._get_remote_paginated(url, offset, limit).await?;
            if total.is_none() {
                total.replace(response.pagination.total);
            }
            if response.data.len() == 0 {
                return Ok(result);
            }
            offset += response.data.len();
            result.append(&mut response.data);
        }
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn post_empty<T: DeserializeOwned>(&self, url: &Url) -> Result<Vec<T>, Error> {
        let resp = self
            .remote_session
            .post_empty(url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        resp.json().await.map_err(Into::into)
    }

    /// # Errors
    /// Return error if api call fails
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

    /// # Errors
    /// Return error if api call fails
    pub async fn get_local<T: DeserializeOwned + Send + 'static>(
        &self,
        table: &str,
        start_timestamp: Option<OffsetDateTime>,
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

    /// # Errors
    /// Return error if api call fails
    pub async fn run_local_command(&self, args: &[&str]) -> Result<Vec<u8>, Error> {
        self.local_session.run_command(args).await
    }

    /// # Errors
    /// Return error if api call fails
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

    /// # Errors
    /// Return error if api call fails
    pub async fn put_local<T: Serialize>(
        &self,
        table: &str,
        data: &[T],
        start_timestamp: Option<OffsetDateTime>,
    ) -> Result<Vec<u8>, Error> {
        let data = serde_json::to_vec(&data)?;
        self.local_session
            .run_command_import(table, &data, start_timestamp)
            .await
    }
}
