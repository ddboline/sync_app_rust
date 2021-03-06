use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use log::debug;
use maplit::hashmap;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, future::Future};

use stack_string::StackString;

use crate::{
    config::Config, iso_8601_datetime, reqwest_session::ReqwestSession, sync_client::SyncClient,
};

#[derive(Queryable, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarList {
    pub id: i32,
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

#[derive(Queryable, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarCache {
    pub id: i32,
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

pub struct CalendarSync {
    client: SyncClient,
}

impl CalendarSync {
    pub fn new(config: Config) -> Self {
        Self {
            client: SyncClient::new(config, "/usr/bin/calendar-app-rust"),
        }
    }

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
                    let results: HashMap<_, _> = results
                        .into_iter()
                        .map(|val| (val.gcal_id.clone(), val))
                        .collect();
                    results
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
                            (
                                format!("{}_{}", event.gcal_id, event.event_id).into(),
                                event,
                            )
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

        output.extend(Self::get_debug(&measurements2));
        output.extend(Self::get_debug(&measurements3));

        let url = from_url.join(path)?;
        self.client.put_local(table, &measurements2, None).await?;
        self.client
            .put_remote(&url, &measurements3, js_prefix)
            .await?;

        Ok(output)
    }

    fn get_debug<T: Debug>(items: &[T]) -> Vec<StackString> {
        if items.len() < 10 {
            items
                .iter()
                .map(|item| format!("{:?}", item).into())
                .collect()
        } else {
            vec![format!("items {}", items.len()).into()]
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

        output.extend(Self::get_debug(&events2));
        output.extend(Self::get_debug(&events3));

        let url = from_url.join(path)?;
        self.client.put_local(table, &events2, None).await?;
        self.client.put_remote(&url, &events3, js_prefix).await?;

        Ok(output)
    }
}
