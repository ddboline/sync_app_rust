use anyhow::{format_err, Error};
use rand::{
    distr::{Distribution, Uniform},
    rng as thread_rng,
};
use reqwest::{header::HeaderMap, redirect::Policy, Client, Response, Url};
use serde::Serialize;
use std::{collections::HashMap, convert::TryFrom, future::Future, thread::sleep, time::Duration};

#[derive(Debug, Clone)]
pub struct ReqwestSession {
    client: Client,
}

impl ReqwestSession {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(allow_redirects: bool) -> Result<Self, Error> {
        let redirect_policy = if allow_redirects {
            Policy::default()
        } else {
            Policy::none()
        };
        Ok(Self {
            client: Client::builder()
                .cookie_store(true)
                .redirect(redirect_policy)
                .build()?,
        })
    }

    async fn exponential_retry<T, U, V>(f: T) -> Result<U, Error>
    where
        T: Fn() -> V,
        V: Future<Output = Result<U, Error>>,
    {
        let mut timeout: f64 = 1.0;
        let range = Uniform::try_from(0..1000)?;
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

    /// # Errors
    /// Return error if db query fails
    pub async fn get(&self, url: &Url, headers: &HeaderMap) -> Result<Response, Error> {
        Self::exponential_retry(|| async move { self.get_impl(url.clone(), headers.clone()).await })
            .await
    }

    /// # Errors
    /// Return error if db query fails
    async fn get_impl(&self, url: Url, headers: HeaderMap) -> Result<Response, Error> {
        self.client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn post_empty(&self, url: &Url, headers: &HeaderMap) -> Result<Response, Error> {
        Self::exponential_retry(|| async move {
            self.post_empty_impl(url.clone(), headers.clone()).await
        })
        .await
    }

    /// # Errors
    /// Return error if db query fails
    async fn post_empty_impl(&self, url: Url, headers: HeaderMap) -> Result<Response, Error> {
        self.client
            .post(url)
            .headers(headers)
            .send()
            .await
            .map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
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
            self.post_impl(url.clone(), headers.clone(), form).await
        })
        .await
    }

    async fn post_impl<T>(
        &self,
        url: Url,
        headers: HeaderMap,
        form: &HashMap<&str, T>,
    ) -> Result<Response, Error>
    where
        T: Serialize,
    {
        self.client
            .post(url)
            .headers(headers)
            .json(form)
            .send()
            .await
            .map_err(Into::into)
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn delete(&self, url: &Url, headers: &HeaderMap) -> Result<Response, Error> {
        Self::exponential_retry(
            || async move { self.delete_impl(url.clone(), headers.clone()).await },
        )
        .await
    }

    async fn delete_impl(&self, url: Url, headers: HeaderMap) -> Result<Response, Error> {
        self.client
            .delete(url)
            .headers(headers)
            .send()
            .await
            .map_err(Into::into)
    }
}
