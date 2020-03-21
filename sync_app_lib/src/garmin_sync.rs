use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use log::debug;
use maplit::hashmap;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, future::Future};

use super::{
    config::Config,
    iso_8601_datetime,
    reqwest_session::{ReqwestSession, SyncClient},
};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StravaItem {
    #[serde(with = "iso_8601_datetime")]
    pub begin_datetime: DateTime<Utc>,
    pub title: String,
}

pub struct GarminSync {
    client: SyncClient,
}

impl GarminSync {
    pub fn new(config: Config) -> Self {
        Self {
            client: SyncClient::new(config),
        }
    }

    pub async fn run_sync(&self) -> Result<Vec<String>, Error> {
        self.init("garmin").await?;
        let mut output = Vec::new();
        let results = self
            .run_single_sync_scale_measurement(
                "garmin/scale_measurements",
                "measurements",
                |resp| {
                    let url = resp.url().clone();
                    async move {
                        let results: Vec<ScaleMeasurement> = resp.json().await?;
                        debug!("measurements {} {}", url, results.len());
                        let results: HashMap<_, _> =
                            results.into_iter().map(|val| (val.datetime, val)).collect();
                        Ok(results)
                    }
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities("garmin/strava/activities_db", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let results: HashMap<String, StravaItem> = resp.json().await?;
                    debug!("activities {} {}", url, results.len());
                    Ok(results)
                }
            })
            .await?;
        output.extend_from_slice(&results);

        Ok(output)
    }

    async fn run_single_sync_scale_measurement<F, T>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: T,
    ) -> Result<Vec<String>, Error>
    where
        T: FnMut(Response) -> F,
        F: Future<Output = Result<HashMap<DateTime<Utc>, ScaleMeasurement>, Error>>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join(path)?;
        let measurements0 =
            transform(self.client.session0.get(&url, &HeaderMap::new()).await?).await?;
        let url = to_url.join(path)?;
        let measurements1 =
            transform(self.client.session1.get(&url, &HeaderMap::new()).await?).await?;

        output.extend_from_slice(&[self
            .combine_measurements(
                &measurements0,
                &measurements1,
                path,
                js_prefix,
                &to_url,
                &self.client.session1,
            )
            .await?]);
        output.extend_from_slice(&[self
            .combine_measurements(
                &measurements1,
                &measurements0,
                path,
                js_prefix,
                &from_url,
                &self.client.session0,
            )
            .await?]);

        Ok(output)
    }

    async fn combine_measurements(
        &self,
        measurements0: &HashMap<DateTime<Utc>, ScaleMeasurement>,
        measurements1: &HashMap<DateTime<Utc>, ScaleMeasurement>,
        path: &str,
        js_prefix: &str,
        to_url: &Url,
        session: &ReqwestSession,
    ) -> Result<String, Error> {
        let mut output = String::new();
        let measurements: Vec<_> = measurements0
            .iter()
            .filter_map(|(k, v)| {
                if measurements1.contains_key(&k) {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        if !measurements.is_empty() {
            if measurements.len() < 20 {
                output = format!("{:?}", measurements);
            } else {
                output = format!("session1 {}", measurements.len());
            }
            let url = to_url.join(path)?;
            for meas in measurements.chunks(100) {
                let data = hashmap! {
                    js_prefix => meas,
                };
                session
                    .post(&url, &HeaderMap::new(), &data)
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(output)
    }

    async fn run_single_sync_activities<T, F>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: T,
    ) -> Result<Vec<String>, Error>
    where
        T: FnMut(Response) -> F,
        F: Future<Output = Result<HashMap<String, StravaItem>, Error>>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join(path)?;
        let activities0 =
            transform(self.client.session0.get(&url, &HeaderMap::new()).await?).await?;
        let url = to_url.join(path)?;
        let activities1 =
            transform(self.client.session1.get(&url, &HeaderMap::new()).await?).await?;

        output.push(
            self.combine_activities(
                &activities0,
                &activities1,
                &to_url,
                path,
                js_prefix,
                &self.client.session1,
            )
            .await?,
        );
        output.push(
            self.combine_activities(
                &activities1,
                &activities0,
                &from_url,
                path,
                js_prefix,
                &self.client.session0,
            )
            .await?,
        );

        Ok(output)
    }

    async fn combine_activities(
        &self,
        activities0: &HashMap<String, StravaItem>,
        activities1: &HashMap<String, StravaItem>,
        to_url: &Url,
        path: &str,
        js_prefix: &str,
        session: &ReqwestSession,
    ) -> Result<String, Error> {
        let mut output = String::new();
        let activities: Vec<_> = activities0
            .iter()
            .filter_map(|(k, v)| {
                if activities1.contains_key(k.as_str()) {
                    None
                } else {
                    Some((k, v))
                }
            })
            .collect();
        if !activities.is_empty() {
            if activities.len() < 20 {
                output = format!("{:?}", activities);
            } else {
                output = format!("session1 {}", activities.len());
            }
            let url = to_url.join(path)?;
            for activity in activities.chunks(100) {
                let act: HashMap<String, StravaItem> = activity
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).clone()))
                    .collect();
                let data = hashmap! {
                    js_prefix => act,
                };
                session
                    .post(&url, &HeaderMap::new(), &data)
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(output)
    }
}
