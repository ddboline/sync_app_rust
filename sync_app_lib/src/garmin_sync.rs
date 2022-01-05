use anyhow::{format_err, Error};
use chrono::{DateTime, NaiveDate, Utc};
use core::hash::Hash;
use maplit::hashmap;
use postgres_query::FromSqlRow;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Write},
    future::Future,
};

use stack_string::StackString;

use super::{
    config::Config, iso_8601_datetime, reqwest_session::ReqwestSession, sync_client::SyncClient,
};

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
struct ScaleMeasurement {
    pub id: i32,
    pub datetime: DateTime<Utc>,
    pub mass: f64,
    pub fat_pct: f64,
    pub water_pct: f64,
    pub muscle_pct: f64,
    pub bone_pct: f64,
}

#[derive(Serialize, Deserialize, FromSqlRow, Debug, Clone)]
pub struct StravaActivity {
    pub id: i64,
    pub name: StackString,
    pub start_date: DateTime<Utc>,
    pub distance: Option<f64>,
    pub moving_time: Option<i64>,
    pub elapsed_time: i64,
    pub total_elevation_gain: Option<f64>,
    pub elev_high: Option<f64>,
    pub elev_low: Option<f64>,
    pub activity_type: StackString,
    pub timezone: StackString,
}

#[derive(Serialize, Deserialize, Clone, Debug, FromSqlRow)]
pub struct FitbitActivityEntry {
    log_id: i64,
    log_type: StackString,
    start_time: DateTime<Utc>,
    tcx_link: Option<StackString>,
    activity_type_id: Option<i64>,
    activity_name: Option<StackString>,
    duration: i64,
    distance: Option<f64>,
    distance_unit: Option<StackString>,
    steps: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug, FromSqlRow, Clone)]
pub struct GarminConnectActivity {
    pub activity_id: i64,
    pub activity_name: Option<StackString>,
    pub description: Option<StackString>,
    pub start_time_gmt: DateTime<Utc>,
    pub distance: Option<f64>,
    pub duration: f64,
    pub elapsed_duration: Option<f64>,
    pub moving_duration: Option<f64>,
    pub steps: Option<i64>,
    pub calories: Option<f64>,
    pub average_hr: Option<f64>,
    pub max_hr: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromSqlRow, PartialEq)]
pub struct RaceResults {
    pub id: i64,
    pub race_type: String,
    pub race_date: Option<NaiveDate>,
    pub race_name: Option<StackString>,
    pub race_distance: i32, // distance in meters
    pub race_time: f64,
    pub race_flag: bool,
    pub race_summary_ids: Vec<Option<i32>>,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, FromSqlRow)]
pub struct FitbitStatisticsSummary {
    pub date: NaiveDate,
    pub min_heartrate: f64,
    pub max_heartrate: f64,
    pub mean_heartrate: f64,
    pub median_heartrate: f64,
    pub stdev_heartrate: f64,
    pub number_of_entries: i32,
}

#[derive(Clone)]
pub struct GarminSync {
    client: SyncClient,
}

impl GarminSync {
    pub fn new(config: Config) -> Self {
        Self {
            client: SyncClient::new(config, "/usr/bin/garmin-rust-cli"),
        }
    }

    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("garmin").await?;
        let mut output = Vec::new();
        let results = self
            .run_single_sync_scale_measurement(
                "garmin/scale_measurements",
                "measurements",
                "scale_measurements",
                |measurements| {
                    {
                        measurements
                            .into_iter()
                            .map(|val| (val.datetime, val))
                            .collect()
                    }
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities(
                "garmin/strava/activities_db",
                "updates",
                "strava_activities",
                |items: Vec<StravaActivity>| {
                    items
                        .into_iter()
                        .map(|activity| (activity.id, activity))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities(
                "garmin/fitbit/fitbit_activities_db",
                "updates",
                "fitbit_activities",
                |items: Vec<FitbitActivityEntry>| {
                    items
                        .into_iter()
                        .map(|activity| (activity.log_id, activity))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities(
                "garmin/fitbit/heartrate_statistics_summary_db",
                "updates",
                "heartrate_statistics_summary",
                |items: Vec<FitbitStatisticsSummary>| {
                    items.into_iter().map(|item| (item.date, item)).collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_activities(
                "garmin/garmin_connect_activities_db",
                "updates",
                "garmin_connect_activities",
                |items: Vec<GarminConnectActivity>| {
                    {
                        items
                            .into_iter()
                            .map(|activity| (activity.activity_id, activity))
                            .collect()
                    }
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_race_results("garmin/race_results_db", "updates", "race_results")
            .await?;
        output.extend_from_slice(&results);

        self.client.shutdown().await?;

        Ok(output)
    }

    async fn run_single_sync_scale_measurement<T>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        T: FnMut(Vec<ScaleMeasurement>) -> HashMap<DateTime<Utc>, ScaleMeasurement>,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let measurements0 = transform(self.client.get_remote(&url).await?);
        let measurements1 = transform(self.client.get_local(table, None).await?);

        let measurements2 = Self::combine_measurements(&measurements0, &measurements1);
        let measurements3 = Self::combine_measurements(&measurements1, &measurements0);

        output.extend(Self::get_debug(table, &measurements2));
        output.extend(Self::get_debug(table, &measurements3));

        let url = from_url.join(path)?;
        self.client.put_local(table, &measurements2, None).await?;
        self.client
            .put_remote(&url, &measurements3, js_prefix)
            .await?;

        Ok(output)
    }

    fn get_debug<T: Debug>(label: &str, items: &[T]) -> Vec<StackString> {
        if items.len() < 10 {
            items
                .iter()
                .map(|item| {
                    let mut buf = StackString::new();
                    write!(buf, "{} {:?}", label, item).unwrap();
                    buf
                })
                .collect()
        } else {
            vec![{
                let mut buf = StackString::new();
                write!(buf, "{} items {}", label, items.len()).unwrap();
                buf
            }]
        }
    }

    fn combine_measurements<'a, T>(
        measurements0: &'a HashMap<DateTime<Utc>, T>,
        measurements1: &'a HashMap<DateTime<Utc>, T>,
    ) -> Vec<&'a T> {
        measurements0
            .iter()
            .filter_map(|(k, v)| {
                if measurements1.contains_key(k) {
                    None
                } else {
                    Some(v)
                }
            })
            .collect()
    }

    async fn run_single_sync_activities<K, T, U>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        K: Hash + Ord,
        T: FnMut(Vec<U>) -> HashMap<K, U>,
        U: DeserializeOwned + Send + Debug + Serialize + 'static,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let activities0 = transform(self.client.get_remote(&url).await?);
        let activities1 = transform(self.client.get_local(table, None).await?);

        let activities2 = Self::combine_activities(&activities0, &activities1);
        let activities3 = Self::combine_activities(&activities1, &activities0);

        output.extend(Self::get_debug(table, &activities2));
        output.extend(Self::get_debug(table, &activities3));

        let url = from_url.join(path)?;
        self.client.put_local(table, &activities2, None).await?;
        self.client
            .put_remote(&url, &activities3, js_prefix)
            .await?;

        Ok(output)
    }

    fn combine_activities<'a, K, T>(
        measurements0: &'a HashMap<K, T>,
        measurements1: &'a HashMap<K, T>,
    ) -> Vec<&'a T>
    where
        K: Hash + Ord,
    {
        measurements0
            .iter()
            .filter_map(|(k, v)| {
                if measurements1.contains_key(k) {
                    None
                } else {
                    Some(v)
                }
            })
            .collect()
    }

    async fn run_single_sync_race_results(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
    ) -> Result<Vec<StackString>, Error> {
        fn transform_personal(
            activities: &[RaceResults],
        ) -> HashMap<(&StackString, NaiveDate), &RaceResults> {
            activities
                .iter()
                .filter_map(|result| {
                    if result.race_type == "personal" {
                        let race_name = result.race_name.as_ref().unwrap();
                        let race_date = result.race_date.unwrap();
                        Some(((race_name, race_date), result))
                    } else {
                        None
                    }
                })
                .collect()
        }
        fn transform_world_record<'a>(
            activities: &'a [RaceResults],
            race_type: &'a str,
        ) -> HashMap<i64, &'a RaceResults> {
            activities
                .iter()
                .filter_map(|result| {
                    if result.race_type == race_type {
                        Some((i64::from(result.race_distance), result))
                    } else {
                        None
                    }
                })
                .collect()
        }

        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let activities0: Vec<RaceResults> = self.client.get_remote(&url).await?;
        let activities1: Vec<RaceResults> = self.client.get_local(table, None).await?;

        {
            let activities0 = transform_personal(&activities0);
            let activities1 = transform_personal(&activities1);
            let activities2 = Self::combine_personal_race_results(&activities0, &activities1);
            let activities3 = Self::combine_personal_race_results(&activities1, &activities0);
            output.extend(Self::get_debug(table, &activities2));
            output.extend(Self::get_debug(table, &activities3));

            let url = from_url.join(path)?;
            self.client.put_local(table, &activities2, None).await?;
            self.client
                .put_remote(&url, &activities3, js_prefix)
                .await?;
        }

        for race_type in &["world_record_men", "world_record_women"] {
            let activities0 = transform_world_record(&activities0, race_type);
            let activities1 = transform_world_record(&activities1, race_type);
            let activities2 = Self::combine_activities(&activities0, &activities1);
            let activities3 = Self::combine_activities(&activities1, &activities0);
            output.extend(Self::get_debug(race_type, &activities2));
            output.extend(Self::get_debug(race_type, &activities3));

            let url = from_url.join(path)?;
            self.client.put_local(table, &activities2, None).await?;
            self.client
                .put_remote(&url, &activities3, js_prefix)
                .await?;
        }

        Ok(output)
    }

    fn combine_personal_race_results<'a, T>(
        race_results0: &'a HashMap<(&'a StackString, NaiveDate), &'a T>,
        race_results1: &'a HashMap<(&'a StackString, NaiveDate), &'a T>,
    ) -> Vec<&'a T> {
        race_results0
            .iter()
            .filter_map(|(k, v)| {
                if race_results1.contains_key(k) {
                    None
                } else {
                    Some(*v)
                }
            })
            .collect()
    }
}
