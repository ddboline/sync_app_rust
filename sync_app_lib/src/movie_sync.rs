#![allow(clippy::too_many_arguments)]

use anyhow::{format_err, Error};
use chrono::{DateTime, NaiveDate, Utc};
use log::debug;
use maplit::hashmap;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, future::Future};

use stack_string::StackString;

use crate::{config::Config, reqwest_session::ReqwestSession, sync_client::SyncClient};

#[derive(Deserialize)]
struct LastModifiedStruct {
    table: StackString,
    last_modified: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ImdbEpisodes {
    pub show: StackString,
    pub title: StackString,
    pub season: i32,
    pub episode: i32,
    pub airdate: NaiveDate,
    pub rating: f64,
    pub eptitle: StackString,
    pub epurl: StackString,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ImdbRatings {
    pub index: i32,
    pub show: StackString,
    pub title: Option<StackString>,
    pub link: StackString,
    pub rating: Option<f64>,
    pub istv: Option<bool>,
    pub source: Option<StackString>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct MovieCollectionRow {
    pub idx: i32,
    pub path: StackString,
    pub show: StackString,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct MovieQueueRow {
    pub idx: i32,
    pub collection_idx: i32,
    pub path: StackString,
    pub show: StackString,
}

pub struct MovieSync {
    client: SyncClient,
}

impl MovieSync {
    pub fn new(config: Config) -> Self {
        Self {
            client: SyncClient::new(config, "/usr/bin/movie-queue-cli"),
        }
    }

    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("list").await?;
        let mut output = Vec::new();

        let from_url = self.client.get_url()?;
        let url = from_url.join("list/last_modified")?;
        debug!("{}", url);
        let last_modified0 = Self::get_last_modified(&url, &self.client.session0).await?;
        let last_modified1 =
            Self::transform_last_modified(self.client.get_local("last_modified", None).await?);

        debug!("{:?} {:?}", last_modified0, last_modified1);

        macro_rules! sync_single_table {
            ($table:expr, $js_prefix:expr, $T:ty) => {{
                let table = $table;
                let js_prefix = $js_prefix;
                debug!("{} {}", table, js_prefix);
                let now = Utc::now();
                let last_mod0 = last_modified0.get(table).unwrap_or_else(|| &now);
                let last_mod1 = last_modified1.get(table).unwrap_or_else(|| &now);
                let last_mod = if last_mod0 < last_mod1 {
                    *last_mod0
                } else {
                    *last_mod1
                };
                let results = self
                    .run_single_sync::<$T>(table, last_mod, js_prefix)
                    .await?;
                output.extend_from_slice(&results);
            }};
        }

        sync_single_table!("imdb_ratings", "shows", ImdbRatings);
        sync_single_table!("imdb_episodes", "episodes", ImdbEpisodes);
        sync_single_table!("movie_collection", "collection", MovieCollectionRow);
        sync_single_table!("movie_queue", "queue", MovieQueueRow);

        Ok(output)
    }

    async fn get_last_modified(
        url: &Url,
        session: &ReqwestSession,
    ) -> Result<HashMap<StackString, DateTime<Utc>>, Error> {
        let last_modified: Vec<LastModifiedStruct> =
            session.get(url, &HeaderMap::new()).await?.json().await?;
        let results = Self::transform_last_modified(last_modified);
        Ok(results)
    }

    fn transform_last_modified(
        data: Vec<LastModifiedStruct>,
    ) -> HashMap<StackString, DateTime<Utc>> {
        data.into_iter()
            .map(|entry| (entry.table, entry.last_modified))
            .collect()
    }

    async fn run_single_sync<T>(
        &self,
        table: &str,
        last_modified: DateTime<Utc>,
        js_prefix: &str,
    ) -> Result<Vec<StackString>, Error>
    where
        T: Debug + Serialize + DeserializeOwned + Send + 'static,
    {
        let from_url = self.client.get_url()?;
        self._run_single_sync::<T>(&from_url, table, last_modified, js_prefix)
            .await
    }

    async fn _run_single_sync<T>(
        &self,
        endpoint: &Url,
        table: &str,
        last_modified: DateTime<Utc>,
        js_prefix: &str,
    ) -> Result<Vec<StackString>, Error>
    where
        T: Debug + Serialize + DeserializeOwned + Send + 'static,
    {
        let mut output = Vec::new();

        let path = format!(
            "list/{}?start_timestamp={}",
            table,
            last_modified.format("%Y-%m-%dT%H:%M:%S%.fZ")
        );
        let url = endpoint.join(&path)?;
        debug!("{}", url);
        output.push(format!("{}", url).into());
        let remote_data: Vec<T> = self.client.get_remote(&url).await?;
        let local_data: Vec<T> = self.client.get_local(table, Some(last_modified)).await?;

        debug!("{:#?}", remote_data);
        self.client
            .put_local(table, &remote_data, Some(last_modified))
            .await?;

        let path = format!("list/{}", table);
        let url = endpoint.join(&path)?;
        debug!("{} {:#?}", url, local_data);
        self.client.put_remote(&url, &local_data, js_prefix).await?;

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use log::debug;

    use crate::{config::Config, movie_sync::MovieSync};

    #[tokio::test]
    #[ignore]
    async fn test_movie_sync() {
        let config = Config::init_config().unwrap();
        let s = MovieSync::new(config);
        s.client.init("list").await.unwrap();
        let result = s.run_sync().await.unwrap();
        debug!("{:?}", result);
        assert!(result.len() > 0);
    }
}
