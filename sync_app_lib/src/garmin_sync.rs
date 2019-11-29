use chrono::{DateTime, Utc};
use failure::{err_msg, Error};
use maplit::hashmap;
use reqwest::header::HeaderMap;
use reqwest::{Response, Url};
use serde::{Deserialize, Serialize};
use std::collections::{ HashMap};
use std::fmt::Debug;

use super::config::Config;
use super::reqwest_session::ReqwestSession;

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
struct ScaleMeasurement {
    pub datetime: DateTime<Utc>,
    pub mass: f64,
    pub fat_pct: f64,
    pub water_pct: f64,
    pub muscle_pct: f64,
    pub bone_pct: f64,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
struct FitbitHeartRate {
    pub datetime: DateTime<Utc>,
    pub value: i32,
}

pub struct GarminSync {
    session0: ReqwestSession,
    session1: ReqwestSession,
    config: Config,
}

impl GarminSync {
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
            .ok_or_else(|| err_msg("No From URL"))?
            .parse()?;
        let to_url: Url = self
            .config
            .garmin_to_url
            .as_ref()
            .ok_or_else(|| err_msg("No To URL"))?
            .parse()?;
        Ok((from_url, to_url))
    }

    pub fn init(&self) -> Result<(), Error> {
        let (from_url, to_url) = self.get_urls()?;
        let user = self
            .config
            .garmin_username
            .as_ref()
            .ok_or_else(|| err_msg("No Username"))?;
        let password = self
            .config
            .garmin_password
            .as_ref()
            .ok_or_else(|| err_msg("No Password"))?;

        let data = hashmap! {
            "email" => user.as_str(),
            "password" => password.as_str(),
        };

        let url = from_url.join("api/auth")?;
        let resp = self
            .session0
            .post(&url, HeaderMap::new(), &data)?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        let url = to_url.join("api/auth")?;
        let resp = self
            .session1
            .post(&url, HeaderMap::new(), &data)?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        Ok(())
    }

    pub fn run_sync(&self) -> Result<Vec<String>, Error> {
        self.init()?;
        let mut output = Vec::new();
        let results =
            self.run_single_sync("garmin/scale_measurements", "measurements", |mut resp| {
                let results: Vec<ScaleMeasurement> = resp.json()?;
                output.push(format!("measurements {} {}", resp.url(), results.len()));
                let results: HashMap<_, _> =
                    results.into_iter().map(|val| (val.datetime, val)).collect();
                Ok(results)
            })?;
        output.extend_from_slice(&results);

        Ok(output)
    }

    fn run_single_sync<T, F>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: F,
    ) -> Result<Vec<String>, Error>
    where
        T: Debug + Serialize,
        F: FnMut(Response) -> Result<HashMap<DateTime<Utc>, T>, Error>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join(path)?;
        let measurements0: HashMap<DateTime<Utc>, T> =
            transform(self.session0.get(&url, HeaderMap::new())?)?;
        let url = to_url.join(path)?;
        let measurements1: HashMap<DateTime<Utc>, T> =
            transform(self.session1.get(&url, HeaderMap::new())?)?;

        let measurements: Vec<_> = measurements0
            .iter()
            .filter(|(k, _)| !measurements1.contains_key(&k))
            .map(|(_, v)| v)
            .collect();
        if !measurements.is_empty() {
            if measurements.len() < 20 {
                output.push(format!("{:?}", measurements));
            } else {
                output.push(format!("session1 {}", measurements.len()));
            }
            let url = to_url.join(path)?;
            for meas in measurements.chunks(100) {
                let data = hashmap! {
                    js_prefix => meas,
                };
                self.session1
                    .post(&url, HeaderMap::new(), &data)?
                    .error_for_status()?;
            }
        }
        let measurements: Vec<_> = measurements1
            .iter()
            .filter(|(k, _)| !measurements0.contains_key(&k))
            .map(|(_, v)| v)
            .collect();
        if !measurements.is_empty() {
            if measurements.len() < 20 {
                output.push(format!("{:?}", measurements));
            } else {
                output.push(format!("session0 {}", measurements.len()));
            }
            let url = from_url.join(path)?;
            for meas in measurements.chunks(100) {
                let data = hashmap! {
                    js_prefix => meas,
                };
                self.session0
                    .post(&url, HeaderMap::new(), &data)?
                    .error_for_status()?;
            }
        }
        Ok(output)
    }
}
