use anyhow::{format_err, Error};
use chrono::{DateTime, Utc};
use log::debug;
use maplit::hashmap;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, future::Future};

use super::{config::Config, iso_8601_datetime, reqwest_session::ReqwestSession};

#[derive(Queryable, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarList {
    pub id: i32,
    pub calendar_name: String,
    pub gcal_id: String,
    pub gcal_name: Option<String>,
    pub gcal_description: Option<String>,
    pub gcal_location: Option<String>,
    pub gcal_timezone: Option<String>,
    pub sync: bool,
    pub last_modified: DateTime<Utc>,
}

#[derive(Queryable, Clone, Debug, Serialize, Deserialize)]
pub struct CalendarCache {
    pub id: i32,
    pub gcal_id: String,
    pub event_id: String,
    pub event_start_time: DateTime<Utc>,
    pub event_end_time: DateTime<Utc>,
    pub event_url: Option<String>,
    pub event_name: String,
    pub event_description: Option<String>,
    pub event_location_name: Option<String>,
    pub event_location_lat: Option<f64>,
    pub event_location_lon: Option<f64>,
    pub last_modified: DateTime<Utc>,
}

pub struct CalendarSync {
    session0: ReqwestSession,
    session1: ReqwestSession,
    config: Config,
}

impl CalendarSync {
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
            .ok_or_else(|| format_err!("No From URL"))?
            .parse()?;
        let to_url: Url = self
            .config
            .garmin_to_url
            .as_ref()
            .ok_or_else(|| format_err!("No To URL"))?
            .parse()?;
        Ok((from_url, to_url))
    }

    pub async fn init(&self) -> Result<(), Error> {
        let (from_url, to_url) = self.get_urls()?;
        let user = self
            .config
            .garmin_username
            .as_ref()
            .ok_or_else(|| format_err!("No Username"))?;
        let password = self
            .config
            .garmin_password
            .as_ref()
            .ok_or_else(|| format_err!("No Password"))?;

        let data = hashmap! {
            "email" => user.as_str(),
            "password" => password.as_str(),
        };

        let url = from_url.join("api/auth")?;
        let resp = self
            .session0
            .post(&url, &HeaderMap::new(), &data)
            .await?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        let url = to_url.join("api/auth")?;
        let resp = self
            .session1
            .post(&url, &HeaderMap::new(), &data)
            .await?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        Ok(())
    }

    pub async fn run_sync(&self) -> Result<Vec<String>, Error> {
        self.init().await?;
        let mut output = Vec::new();
        let results = self
            .run_single_sync_calendar_list("calendar/calendar_list", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let results: Vec<CalendarList> = resp.json().await?;
                    debug!("calendars {} {}", url, results.len());
                    let results: HashMap<_, _> = results
                        .into_iter()
                        .map(|val| (val.gcal_id.to_string(), val))
                        .collect();
                    Ok(results)
                }
            })
            .await?;
        output.extend_from_slice(&results);

        let results = self
            .run_single_sync_calendar_events("calendar/calendar_cache", "updates", |resp| {
                let url = resp.url().clone();
                async move {
                    let results: Vec<CalendarCache> = resp.json().await?;
                    let results: HashMap<String, CalendarCache> = results
                        .into_iter()
                        .map(|event| (format!("{}_{}", event.gcal_id, event.event_id), event))
                        .collect();
                    debug!("activities {} {}", url, results.len());
                    Ok(results)
                }
            })
            .await?;
        output.extend_from_slice(&results);

        Ok(output)
    }

    async fn run_single_sync_calendar_list<F, T>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: T,
    ) -> Result<Vec<String>, Error>
    where
        T: FnMut(Response) -> F,
        F: Future<Output = Result<HashMap<String, CalendarList>, Error>>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join(path)?;
        let measurements0 = transform(self.session0.get(&url, &HeaderMap::new()).await?).await?;
        let url = to_url.join(path)?;
        let measurements1 = transform(self.session1.get(&url, &HeaderMap::new()).await?).await?;

        output.extend_from_slice(&[self
            .combine_measurements(
                &measurements0,
                &measurements1,
                path,
                js_prefix,
                &to_url,
                &self.session1,
            )
            .await?]);
        output.extend_from_slice(&[self
            .combine_measurements(
                &measurements1,
                &measurements0,
                path,
                js_prefix,
                &from_url,
                &self.session0,
            )
            .await?]);

        Ok(output)
    }

    async fn combine_measurements(
        &self,
        measurements0: &HashMap<String, CalendarList>,
        measurements1: &HashMap<String, CalendarList>,
        path: &str,
        js_prefix: &str,
        to_url: &Url,
        session: &ReqwestSession,
    ) -> Result<String, Error> {
        let mut output = String::new();
        let measurements: Vec<_> = measurements0
            .iter()
            .filter_map(|(k, v)| {
                if measurements1.contains_key(k) {
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

    async fn run_single_sync_calendar_events<T, F>(
        &self,
        path: &str,
        js_prefix: &str,
        mut transform: T,
    ) -> Result<Vec<String>, Error>
    where
        T: FnMut(Response) -> F,
        F: Future<Output = Result<HashMap<String, CalendarCache>, Error>>,
    {
        let mut output = Vec::new();
        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join(path)?;
        let events0 = transform(self.session0.get(&url, &HeaderMap::new()).await?).await?;
        let url = to_url.join(path)?;
        let events1 = transform(self.session1.get(&url, &HeaderMap::new()).await?).await?;

        output.push(
            self.combine_events(&events0, &events1, &to_url, path, js_prefix, &self.session1)
                .await?,
        );
        output.push(
            self.combine_events(
                &events1,
                &events0,
                &from_url,
                path,
                js_prefix,
                &self.session0,
            )
            .await?,
        );

        Ok(output)
    }

    async fn combine_events(
        &self,
        events0: &HashMap<String, CalendarCache>,
        events1: &HashMap<String, CalendarCache>,
        to_url: &Url,
        path: &str,
        js_prefix: &str,
        session: &ReqwestSession,
    ) -> Result<String, Error> {
        let mut output = String::new();
        let events: Vec<_> = events0
            .iter()
            .filter_map(|(k, v)| {
                if events1.contains_key(k.as_str()) {
                    None
                } else {
                    Some((k, v))
                }
            })
            .collect();
        if !events.is_empty() {
            if events.len() < 20 {
                output = format!("{:?}", events);
            } else {
                output = format!("session1 {}", events.len());
            }
            let url = to_url.join(path)?;
            for activity in events.chunks(100) {
                let act: HashMap<String, CalendarCache> = activity
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
