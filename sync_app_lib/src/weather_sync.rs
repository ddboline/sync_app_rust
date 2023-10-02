use anyhow::Error;
use postgres_query::FromSqlRow;
use serde::{Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
};
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use crate::{config::Config, sync_client::SyncClient};

#[derive(FromSqlRow, Clone, Debug, Serialize, Deserialize)]
pub struct WeatherDataDB {
    pub id: Uuid,
    dt: i32,
    created_at: DateTimeWrapper,
    location_name: StackString,
    latitude: f64,
    longitude: f64,
    condition: StackString,
    temperature: f64,
    temperature_minimum: f64,
    temperature_maximum: f64,
    pressure: f64,
    humidity: i32,
    visibility: Option<f64>,
    rain: Option<f64>,
    snow: Option<f64>,
    wind_speed: f64,
    wind_direction: Option<f64>,
    country: StackString,
    sunrise: DateTimeWrapper,
    sunset: DateTimeWrapper,
    timezone: i32,
    server: StackString,
}

impl fmt::Display for WeatherDataDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.dt, self.location_name)
    }
}

pub struct WeatherSync {
    client: SyncClient,
}

impl WeatherSync {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: Config) -> Result<Self, Error> {
        Ok(Self {
            client: SyncClient::new(config, "/usr/bin/weather-api-rust")?,
        })
    }

    /// # Errors
    /// Return error if sync fails
    #[allow(clippy::similar_names)]
    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("weather", "weather-sync").await?;
        let mut output = Vec::new();

        let results = self
            .run_single_sync_weather_data("weather/history", "updates", "weather_data", |results| {
                results
                    .into_iter()
                    .map(|event| {
                        let key = format_sstr!("{event}");
                        (key, event)
                    })
                    .collect()
            })
            .await?;
        output.extend_from_slice(&results);

        self.client.shutdown().await?;

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

    #[allow(clippy::similar_names)]
    fn combine_maps<'a, T>(
        measurements0: &'a HashMap<StackString, T>,
        measurements1: &'a HashMap<StackString, T>,
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

    #[allow(clippy::similar_names)]
    async fn run_single_sync_weather_data<T>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        T: FnMut(Vec<WeatherDataDB>) -> HashMap<StackString, WeatherDataDB>,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let events0 = transform(self.client.get_remote(&url).await?);
        let start_timestamp = OffsetDateTime::now_utc() - Duration::days(7);
        let events1 = transform(self.client.get_local(table, Some(start_timestamp)).await?);

        let events2 = Self::combine_maps(&events0, &events1);
        let events3 = Self::combine_maps(&events1, &events0);

        output.extend(Self::get_debug(table, &events2));
        output.extend(Self::get_debug(table, &events3));

        let url = from_url.join(path)?;
        self.client.put_local(table, &events2, None).await?;
        self.client.put_remote(&url, &events3, js_prefix).await?;

        Ok(output)
    }
}
