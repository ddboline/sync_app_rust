use anyhow::{format_err, Error};
use arc_swap::ArcSwap;
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

#[derive(Debug, Clone)]
struct ReqwestSessionInner {
    client: Client,
    headers: HeaderMap,
}

pub struct ReqwestSession {
    client: ArcSwap<ReqwestSessionInner>,
}

impl Clone for ReqwestSession {
    fn clone(&self) -> Self {
        Self {
            client: ArcSwap::from(self.client.load().clone())
        }
    }
}

impl Default for ReqwestSession {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ReqwestSessionInner {
    pub async fn get(&self, url: Url, mut headers: HeaderMap) -> Result<Response, Error> {
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
        &self,
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
            client: ArcSwap::new(Arc::new(ReqwestSessionInner {
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
                .load()
                .clone()
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
                .load()
                .post(url.clone(), headers.clone(), form)
                .await
        })
        .await
    }

    pub async fn set_default_headers(&self, headers: HashMap<&str, &str>) -> Result<(), Error> {
        let mut client = self.client.load().clone();
        for (k, v) in headers {
            let name: HeaderName = k.parse()?;
            let val: HeaderValue = v.parse()?;
            Arc::make_mut(&mut client).headers.insert(name, val);
        }
        self.client.store(client);
        Ok(())
    }
}
