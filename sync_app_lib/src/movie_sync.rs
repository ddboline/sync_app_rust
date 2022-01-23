#![allow(clippy::too_many_arguments)]

use anyhow::{format_err, Error};
use chrono::{DateTime, NaiveDate, Utc};
use log::debug;
use maplit::hashmap;
use postgres_query::FromSqlRow;
use reqwest::{header::HeaderMap, Response, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{
    collections::HashMap,
    fmt::{Debug, Write},
    future::Future,
};

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

#[derive(FromSqlRow, Debug, Serialize, Deserialize)]
pub struct PlexEvent {
    pub event: StackString,
    pub account: StackString,
    pub server: StackString,
    pub player_title: StackString,
    pub player_address: StackString,
    pub title: StackString,
    pub parent_title: Option<StackString>,
    pub grandparent_title: Option<StackString>,
    pub added_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub last_modified: DateTime<Utc>,
    pub metadata_type: Option<StackString>,
    pub section_type: Option<StackString>,
    pub section_title: Option<StackString>,
    pub metadata_key: Option<StackString>,
}

#[derive(FromSqlRow, Default, Debug, Serialize, Deserialize)]
pub struct PlexFilename {
    pub metadata_key: StackString,
    pub filename: StackString,
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
        let last_modified0 = self.get_last_modified(&url).await?;
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
                let results = self
                    .run_single_sync::<$T>(table, *last_mod0, *last_mod1, js_prefix)
                    .await?;
                output.extend_from_slice(&results);
            }};
        }

        sync_single_table!("plex_event", "events", PlexEvent);
        sync_single_table!("plex_filename", "filenames", PlexFilename);
        sync_single_table!("imdb_ratings", "shows", ImdbRatings);
        sync_single_table!("imdb_episodes", "episodes", ImdbEpisodes);
        sync_single_table!("movie_collection", "collection", MovieCollectionRow);
        sync_single_table!("movie_queue", "queue", MovieQueueRow);

        self.client.shutdown().await?;

        Ok(output)
    }

    async fn get_last_modified(
        &self,
        url: &Url,
    ) -> Result<HashMap<StackString, DateTime<Utc>>, Error> {
        let last_modified: Vec<LastModifiedStruct> = self.client.get_remote(url).await?;
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
        last_modified_remote: DateTime<Utc>,
        last_modified_local: DateTime<Utc>,
        js_prefix: &str,
    ) -> Result<Vec<StackString>, Error>
    where
        T: Debug + Serialize + DeserializeOwned + Send + 'static,
    {
        let from_url = self.client.get_url()?;
        self._run_single_sync::<T>(
            &from_url,
            table,
            last_modified_remote,
            last_modified_local,
            js_prefix,
        )
        .await
    }

    async fn _run_single_sync<T>(
        &self,
        endpoint: &Url,
        table: &str,
        last_modified_remote: DateTime<Utc>,
        last_modified_local: DateTime<Utc>,
        js_prefix: &str,
    ) -> Result<Vec<StackString>, Error>
    where
        T: Debug + Serialize + DeserializeOwned + Send + 'static,
    {
        let mut output = Vec::new();
        let path = format_sstr!(
            "list/{}?start_timestamp={}",
            table,
            last_modified_local.format("%Y-%m-%dT%H:%M:%S%.fZ")
        );
        let url = endpoint.join(&path)?;
        debug!("{}", url);
        output.push(url.as_str().into());
        let remote_data: Vec<T> = self.client.get_remote(&url).await?;
        let local_data: Vec<T> = self
            .client
            .get_local(table, Some(last_modified_remote))
            .await?;
        self.client.put_local(table, &remote_data, None).await?;
        let path = format_sstr!("list/{table}");
        let url = endpoint.join(&path)?;
        self.client.put_remote(&url, &local_data, js_prefix).await?;
        let buf = format_sstr!("{} {}", table, local_data.len());
        output.push(buf);

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
