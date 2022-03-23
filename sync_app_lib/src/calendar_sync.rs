use anyhow::Error;
use chrono::{DateTime, Utc};
use log::debug;
use postgres_query::FromSqlRow;
use serde::{Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{
    collections::HashMap,
    fmt::{self, Debug, Write},
};

use crate::{config::Config, sync_client::SyncClient};

#[derive(FromSqlRow, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarList {
    pub calendar_name: StackString,
    pub gcal_id: StackString,
    pub gcal_name: Option<StackString>,
    pub gcal_description: Option<StackString>,
    pub gcal_location: Option<StackString>,
    pub gcal_timezone: Option<StackString>,
    pub sync: bool,
    pub last_modified: DateTime<Utc>,
    pub edit: bool,
    pub display: bool,
}

#[derive(FromSqlRow, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarCache {
    pub gcal_id: StackString,
    pub event_id: StackString,
    pub event_start_time: DateTime<Utc>,
    pub event_end_time: DateTime<Utc>,
    pub event_url: Option<StackString>,
    pub event_name: StackString,
    pub event_description: Option<StackString>,
    pub event_location_name: Option<StackString>,
    pub event_location_lat: Option<f64>,
    pub event_location_lon: Option<f64>,
    pub last_modified: DateTime<Utc>,
}

impl fmt::Display for CalendarCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.gcal_id, self.event_id)
    }
}

pub struct CalendarSync {
    client: SyncClient,
}

impl CalendarSync {
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            client: SyncClient::new(config, "/usr/bin/calendar-app-rust"),
        }
    }

    /// # Errors
    /// Return error if sync fails
    #[allow(clippy::similar_names)]
    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("calendar").await?;
        let mut output = Vec::new();
        let results = self
            .run_single_sync_calendar_list(
                "calendar/calendar_list",
                "updates",
                "calendar_list",
                |results| {
                    debug!("calendars {}", results.len());
                    results
                        .into_iter()
                        .map(|val| (val.gcal_id.clone(), val))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_calendar_events(
                "calendar/calendar_cache",
                "updates",
                "calendar_cache",
                |results| {
                    results
                        .into_iter()
                        .map(|event| {
                            let key = format_sstr!("{event}");
                            (key, event)
                        })
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        self.client.shutdown().await?;

        Ok(output)
    }

    #[allow(clippy::similar_names)]
    async fn run_single_sync_calendar_list<T>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        T: FnMut(Vec<CalendarList>) -> HashMap<StackString, CalendarList>,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let measurements0 = transform(self.client.get_remote(&url).await?);
        let measurements1 = transform(self.client.get_local(table, None).await?);

        let measurements2 = Self::combine_maps(&measurements0, &measurements1);
        let measurements3 = Self::combine_maps(&measurements1, &measurements0);

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
    async fn run_single_sync_calendar_events<T>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        T: FnMut(Vec<CalendarCache>) -> HashMap<StackString, CalendarCache>,
    {
        let mut output = Vec::new();
        let from_url = self.client.get_url()?;

        let url = from_url.join(path)?;
        let events0 = transform(self.client.get_remote(&url).await?);
        let events1 = transform(self.client.get_local(table, None).await?);

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
