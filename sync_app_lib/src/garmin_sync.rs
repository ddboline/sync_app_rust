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

#[derive(Serialize, Deserialize, FromSqlRow, Debug, Clone)]
pub struct StravaActivity {
    pub name: String,
    #[serde(with = "iso_8601_datetime")]
    pub start_date: DateTime<Utc>,
    pub id: i64,
    pub distance: Option<f64>,
    pub moving_time: Option<i64>,
    pub elapsed_time: i64,
    pub total_elevation_gain: Option<f64>,
    pub elev_high: Option<f64>,
    pub elev_low: Option<f64>,
    #[serde(rename = "type")]
    pub activity_type: String,
    pub timezone: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, FromSqlRow)]
pub struct FitbitActivityEntry {
    #[serde(rename = "logType")]
    log_type: String,
    #[serde(rename = "startTime")]
    start_time: DateTime<Utc>,
    #[serde(rename = "tcxLink")]
    tcx_link: Option<String>,
    #[serde(rename = "activityTypeId")]
    activity_type_id: Option<i64>,
    #[serde(rename = "activityName")]
    activity_name: Option<String>,
    duration: i64,
    distance: Option<f64>,
    #[serde(rename = "distanceUnit")]
    distance_unit: Option<String>,
    steps: Option<i64>,
    #[serde(rename = "logId")]
    log_id: i64,
}

#[derive(Serialize, Deserialize, Debug, FromSqlRow, Clone)]
pub struct GarminConnectActivity {
    #[serde(rename = "activityId")]
    pub activity_id: i64,
    #[serde(rename = "activityName")]
    pub activity_name: Option<String>,
    pub description: Option<String>,
    #[serde(rename = "startTimeGMT")]
    pub start_time_gmt: DateTime<Utc>,
    pub distance: Option<f64>,
    pub duration: f64,
    #[serde(rename = "elapsedDuration")]
    pub elapsed_duration: Option<f64>,
    #[serde(rename = "movingDuration")]
    pub moving_duration: Option<f64>,
    pub steps: Option<i64>,
    pub calories: Option<f64>,
    #[serde(rename = "averageHR")]
    pub average_hr: Option<f64>,
    #[serde(rename = "maxHR")]
    pub max_hr: Option<f64>,
}

#[derive(Clone)]
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
        self.client.init("garmin").await?;
        let mut output = Vec::new();
        let results = self
            .run_single_sync_scale_measurement(
                "garmin/scale_measurements",
                "measurements",
                |resp| {
                    let url = resp.url().clone();
                    async move {
                        let measurements: Vec<ScaleMeasurement> = resp.json().await?;
                        debug!("measurements {} {}", url, measurements.len());
                        let measurement_map: HashMap<_, _> = measurements
                            .into_iter()
                            .map(|val| (val.datetime, val))
                            .collect();
                        Ok(measurement_map)
                    }
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities("garmin/strava/activities_db", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let items: Vec<StravaActivity> = resp.json().await?;
                    let item_map: HashMap<i64, StravaActivity> = items
                        .into_iter()
                        .map(|activity| (activity.id, activity))
                        .collect();
                    debug!("activities {} {}", url, item_map.len());
                    Ok(item_map)
                }
            })
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities("garmin/fitbit/fitbit_activities_db", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let items: Vec<FitbitActivityEntry> = resp.json().await?;
                    let item_map: HashMap<i64, FitbitActivityEntry> = items
                        .into_iter()
                        .map(|activity| (activity.log_id, activity))
                        .collect();
                    debug!("activities {} {}", url, item_map.len());
                    Ok(item_map)
                }
            })
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities("garmin/garmin_connect_activities_db", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let items: Vec<GarminConnectActivity> = resp.json().await?;
                    let item_map: HashMap<i64, GarminConnectActivity> = items
                        .into_iter()
                        .map(|activity| (activity.activity_id, activity))
                        .collect();
                    debug!("activities {} {}", url, item_map.len());
                    Ok(item_map)
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
        let (from_url, to_url) = self.client.get_urls()?;

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

    async fn run_single_sync_activities<T, U, F>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: T,
    ) -> Result<Vec<String>, Error>
    where
        T: FnMut(Response) -> F,
        U: Debug + Clone + Serialize,
        F: Future<Output = Result<HashMap<i64, U>, Error>>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.client.get_urls()?;

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

    async fn combine_activities<T>(
        &self,
        activities0: &HashMap<i64, T>,
        activities1: &HashMap<i64, T>,
        to_url: &Url,
        path: &str,
        js_prefix: &str,
        session: &ReqwestSession,
    ) -> Result<String, Error>
    where
        T: Debug + Clone + Serialize,
    {
        let mut output = String::new();
        let activities: Vec<_> = activities0
            .iter()
            .filter_map(|(k, v)| {
                if activities1.contains_key(&k) {
                    None
                } else {
                    Some(v)
                }
            })
            .collect();
        if !activities.is_empty() {
            if activities.len() < 20 {
                output = format!("session1 {:?}", activities);
            } else {
                output = format!("session1 {}", activities.len());
            }
            let url = to_url.join(path)?;
            for activity in activities.chunks(100) {
                let data = hashmap! {
                    js_prefix => activity,
                };
                debug!("data {}", serde_json::to_string(&data)?);
                session
                    .post(&url, &HeaderMap::new(), &data)
                    .await?
                    .error_for_status()?;
            }
        }
        Ok(output)
    }
}
