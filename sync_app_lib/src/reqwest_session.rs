use anyhow::{format_err, Error};
use maplit::hashmap;
use rand::{
    distributions::{Distribution, Uniform},
    thread_rng,
};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    redirect::Policy,
    Client, Response, Url,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap, future::Future, pin::Pin, sync::Arc, thread::sleep, time::Duration,
};
use tokio::sync::Mutex;

use crate::config::Config;

#[derive(Debug)]
struct ReqwestSessionInner {
    client: Client,
    headers: HeaderMap,
}

pub struct ReqwestSession {
    client: Arc<Mutex<ReqwestSessionInner>>,
}

impl Default for ReqwestSession {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ReqwestSessionInner {
    pub async fn get(&mut self, url: Url, mut headers: HeaderMap) -> Result<Response, Error> {
        for (k, v) in &self.headers {
            headers.insert(k, v.into());
        }
        self.client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(Into::into)
    }

    pub async fn post<T>(
        &mut self,
        url: Url,
        mut headers: HeaderMap,
        form: &HashMap<&str, T>,
    ) -> Result<Response, Error>
    where
        T: Serialize,
    {
        for (k, v) in &self.headers {
            headers.insert(k, v.into());
        }
        self.client
            .post(url)
            .headers(headers)
            .json(form)
            .send()
            .await
            .map_err(Into::into)
    }
}

impl ReqwestSession {
    pub fn new(allow_redirects: bool) -> Self {
        let redirect_policy = if allow_redirects {
            Policy::default()
        } else {
            Policy::none()
        };
        Self {
            client: Arc::new(Mutex::new(ReqwestSessionInner {
                client: Client::builder()
                    .cookie_store(true)
                    .redirect(redirect_policy)
                    .build()
                    .expect("Failed to build client"),
                headers: HeaderMap::new(),
            })),
        }
    }

    async fn exponential_retry<T, U, V>(f: T) -> Result<U, Error>
    where
        T: Fn() -> V,
        V: Future<Output = Result<U, Error>>,
    {
        let mut timeout: f64 = 1.0;
        let range = Uniform::from(0..1000);
        loop {
            let resp = f().await;
            match resp {
                Ok(x) => return Ok(x),
                Err(e) => {
                    sleep(Duration::from_millis((timeout * 1000.0) as u64));
                    timeout *= 4.0 * f64::from(range.sample(&mut thread_rng())) / 1000.0;
                    if timeout >= 64.0 {
                        return Err(format_err!(e));
                    }
                }
            }
        }
    }

    pub async fn get(&self, url: &Url, headers: &HeaderMap) -> Result<Response, Error> {
        Self::exponential_retry(|| async move {
            self.client
                .lock()
                .await
                .get(url.clone(), headers.clone())
                .await
        })
        .await
    }

    pub async fn post<T>(
        &self,
        url: &Url,
        headers: &HeaderMap,
        form: &HashMap<&str, T>,
    ) -> Result<Response, Error>
    where
        T: Serialize,
    {
        Self::exponential_retry(|| async move {
            self.client
                .lock()
                .await
                .post(url.clone(), headers.clone(), form)
                .await
        })
        .await
    }

    pub async fn set_default_headers(&self, headers: HashMap<&str, &str>) -> Result<(), Error> {
        for (k, v) in headers {
            let name: HeaderName = k.parse()?;
            let val: HeaderValue = v.parse()?;
            self.client.lock().await.headers.insert(name, val);
        }
        Ok(())
    }
}

pub struct SyncClient {
    pub session0: ReqwestSession,
    pub session1: ReqwestSession,
    pub config: Config,
}

impl SyncClient {
    pub fn new(config: Config) -> Self {
        Self {
            session0: ReqwestSession::new(true),
            session1: ReqwestSession::new(true),
            config,
        }
    }

    pub fn get_urls(&self) -> Result<(Url, Url), Error> {
        let from_url: Url = self
            .config
            .garmin_from_url
            .as_ref()
            .ok_or_else(|| format_err!("No From URL"))?
            .parse()?;
        let to_url: Url = self
            .config
            .garmin_to_url
            .as_ref()
            .ok_or_else(|| format_err!("No To URL"))?
            .parse()?;
        Ok((from_url, to_url))
    }

    pub async fn init(&self) -> Result<(), Error> {
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

        #[derive(Serialize, Deserialize)]
        struct LoggedUser {
            email: String,
        }

        let url = from_url.join("calendar/user")?;
        let resp = self
            .session0
            .get(&url, &HeaderMap::new())
            .await?
            .error_for_status()?;
        let _: LoggedUser = resp
            .json()
            .await
            .map_err(|e| format_err!("Login problem {:?}", e))?;

        let url = to_url.join("calendar/user")?;
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
