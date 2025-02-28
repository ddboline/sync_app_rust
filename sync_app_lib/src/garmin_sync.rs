use anyhow::Error;
use core::hash::Hash;
use postgres_query::FromSqlRow;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fmt::Debug};
use time::Date;
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use super::{config::Config, sync_client::SyncClient};

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
struct ScaleMeasurement {
    pub id: Uuid,
    pub datetime: DateTimeWrapper,
    pub mass: f64,
    pub fat_pct: f64,
    pub water_pct: f64,
    pub muscle_pct: f64,
    pub bone_pct: f64,
    pub connect_primary_key: Option<i64>,
}

#[derive(Serialize, Deserialize, FromSqlRow, Debug, Clone)]
pub struct StravaActivity {
    pub id: i64,
    pub name: StackString,
    pub start_date: DateTimeWrapper,
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
    start_time: DateTimeWrapper,
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
    pub start_time_gmt: DateTimeWrapper,
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
    pub id: Uuid,
    pub race_type: String,
    pub race_date: Option<Date>,
    pub race_name: Option<StackString>,
    pub race_distance: i32, // distance in meters
    pub race_time: f64,
    pub race_flag: bool,
    pub race_summary_ids: Vec<Option<Uuid>>,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, PartialEq, FromSqlRow)]
pub struct FitbitStatisticsSummary {
    pub date: Date,
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
    pub const EXE_PATH: &'static str = "/usr/bin/garmin-rust-cli";

    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: Config) -> Result<Self, Error> {
        Ok(Self {
            client: SyncClient::new(config, Self::EXE_PATH)?,
        })
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        let buf = StackString::from_utf8_vec(self.client.run_local_command(&["sync"]).await?)?;
        let mut output: Vec<StackString> = buf.split('\n').map(Into::into).collect();
        let buf = StackString::from_utf8_vec(self.client.run_local_command(&["proc"]).await?)?;
        output.extend(buf.split('\n').map(Into::into));

        self.client.init("garmin", "garmin-sync").await?;
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
        self.client.shutdown().await?;

        output.extend_from_slice(&results);

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
        T: FnMut(Vec<ScaleMeasurement>) -> HashMap<DateTimeWrapper, ScaleMeasurement>,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let measurements0 = transform(self.client.get_remote_paginated(&url, &[]).await?);
        let measurements1 = transform(self.client.get_local(table, None, None).await?);

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
                .map(|item| format_sstr!("{label} {item:?}"))
                .collect()
        } else {
            vec![{ format_sstr!("{} items {}", label, items.len()) }]
        }
    }

    fn combine_measurements<'a, T>(
        measurements0: &'a HashMap<DateTimeWrapper, T>,
        measurements1: &'a HashMap<DateTimeWrapper, T>,
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
        let activities0 = transform(self.client.get_remote_paginated(&url, &[]).await?);
        let activities1 = transform(self.client.get_local(table, None, None).await?);

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
        ) -> HashMap<(&StackString, Date), &RaceResults> {
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
        let activities1: Vec<RaceResults> = self.client.get_local(table, None, None).await?;

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
        race_results0: &'a HashMap<(&'a StackString, Date), &'a T>,
        race_results1: &'a HashMap<(&'a StackString, Date), &'a T>,
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
