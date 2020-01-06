#![allow(clippy::too_many_arguments)]

use chrono::{DateTime, NaiveDate, Utc};
use failure::{err_msg, Error};
use log::debug;
use maplit::hashmap;
use reqwest::blocking::Response;
use reqwest::header::HeaderMap;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{stdout, Write};

use super::config::Config;
use super::reqwest_session::ReqwestSession;

#[derive(Deserialize)]
struct LastModifiedStruct {
    table: String,
    last_modified: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ImdbEpisodes {
    pub show: String,
    pub title: String,
    pub season: i32,
    pub episode: i32,
    pub airdate: NaiveDate,
    pub rating: f64,
    pub eptitle: String,
    pub epurl: String,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ImdbRatings {
    pub index: i32,
    pub show: String,
    pub title: Option<String>,
    pub link: String,
    pub rating: Option<f64>,
    pub istv: Option<bool>,
    pub source: Option<TvShowSource>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TvShowSource {
    #[serde(rename = "netflix")]
    Netflix,
    #[serde(rename = "hulu")]
    Hulu,
    #[serde(rename = "amazon")]
    Amazon,
    #[serde(rename = "all")]
    All,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct MovieCollectionRow {
    pub idx: i32,
    pub path: String,
    pub show: String,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct MovieQueueRow {
    pub idx: i32,
    pub collection_idx: i32,
    pub path: String,
    pub show: String,
}

pub struct MovieSync {
    session0: ReqwestSession,
    session1: ReqwestSession,
    config: Config,
}

impl MovieSync {
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

        let (from_url, to_url) = self.get_urls()?;

        let url = from_url.join("api/auth")?;
        let resp = self
            .session0
            .post(&url, &HeaderMap::new(), &data)?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        let url = to_url.join("api/auth")?;
        let resp = self
            .session1
            .post(&url, &HeaderMap::new(), &data)?
            .error_for_status()?;
        let _: Vec<_> = resp.cookies().collect();
        Ok(())
    }

    pub fn run_sync(&self) -> Result<Vec<String>, Error> {
        self.init()?;
        let mut output = Vec::new();

        let (from_url, to_url) = self.get_urls()?;
        let url = from_url.join("list/last_modified")?;
        debug!("{}", url);
        let last_modified0 = Self::get_last_modified(&url, &self.session0)?;
        let url = to_url.join("list/last_modified")?;
        debug!("{}", url);
        let last_modified1 = Self::get_last_modified(&url, &self.session1)?;

        debug!("{:?} {:?}", last_modified0, last_modified1);

        macro_rules! sync_single_table {
            ($table:expr, $js_prefix:expr, $T:ty) => {{
                let table = $table;
                let js_prefix = $js_prefix;
                debug!("{} {}", table, js_prefix);
                let now = Utc::now();
                let last_mod0 = last_modified0.get(table).unwrap_or_else(|| &now);
                let last_mod1 = last_modified1.get(table).unwrap_or_else(|| &now);
                let results =
                    self.run_single_sync(table, *last_mod0, *last_mod1, js_prefix, |resp| {
                        let result: Vec<$T> = resp.json()?;
                        Ok(result)
                    })?;
                output.extend_from_slice(&results);
            }};
        }

        sync_single_table!("imdb_ratings", "shows", ImdbRatings);
        sync_single_table!("imdb_episodes", "episodes", ImdbEpisodes);
        sync_single_table!("movie_collection", "collection", MovieCollectionRow);
        sync_single_table!("movie_queue", "queue", MovieQueueRow);

        Ok(output)
    }

    fn get_last_modified(
        url: &Url,
        session: &ReqwestSession,
    ) -> Result<HashMap<String, DateTime<Utc>>, Error> {
        let last_modified: Vec<LastModifiedStruct> = session.get(url, &HeaderMap::new())?.json()?;
        let results = last_modified
            .into_iter()
            .map(|entry| (entry.table, entry.last_modified))
            .collect();
        Ok(results)
    }

    fn run_single_sync<T, F>(
        &self,
        table: &str,
        last_modified0: DateTime<Utc>,
        last_modified1: DateTime<Utc>,
        js_prefix: &str,
        transform: F,
    ) -> Result<Vec<String>, Error>
    where
        T: Debug + Serialize,
        F: FnMut(Response) -> Result<Vec<T>, Error>,
    {
        let (from_url, to_url) = self.get_urls()?;
        if last_modified0 > last_modified1 {
            _run_single_sync(
                &from_url,
                &self.session0,
                &to_url,
                &self.session1,
                table,
                last_modified1,
                js_prefix,
                transform,
            )
        } else {
            _run_single_sync(
                &to_url,
                &self.session1,
                &from_url,
                &self.session0,
                table,
                last_modified0,
                js_prefix,
                transform,
            )
        }
    }
}

fn _run_single_sync<T, F>(
    endpoint0: &Url,
    session0: &ReqwestSession,
    endpoint1: &Url,
    session1: &ReqwestSession,
    table: &str,
    last_modified: DateTime<Utc>,
    js_prefix: &str,
    mut transform: F,
) -> Result<Vec<String>, Error>
where
    T: Debug + Serialize,
    F: FnMut(Response) -> Result<Vec<T>, Error>,
{
    let mut output = Vec::new();

    let path = format!(
        "list/{}?start_timestamp={}",
        table,
        last_modified.format("%Y-%m-%dT%H:%M:%S%.fZ")
    );
    let url = endpoint0.join(&path)?;
    writeln!(stdout(), "{}", url)?;
    output.push(format!("{}", url));
    let data = transform(session0.get(&url, &HeaderMap::new())?)?;

    let path = format!("list/{}", table);
    let url = endpoint1.join(&path)?;
    writeln!(stdout(), "{} {:#?}", url, data)?;
    output.push(format!("{}", url));
    for chunk in data.chunks(100) {
        let js = hashmap! {
            js_prefix=> chunk,
        };
        session1
            .post(&url, &HeaderMap::new(), &js)?
            .error_for_status()?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::io::{stdout, Write};

    use crate::config::Config;
    use crate::movie_sync::MovieSync;

    #[test]
    #[ignore]
    fn test_movie_sync() {
        let config = Config::init_config().unwrap();
        let s = MovieSync::new(config);
        s.init().unwrap();
        let result = s.run_sync().unwrap();
        writeln!(stdout(), "{:?}", result).unwrap();
        assert!(result.len() > 0);
    }
}
