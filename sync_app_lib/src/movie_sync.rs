#![allow(clippy::too_many_arguments)]

use anyhow::Error;
use log::debug;
use postgres_query::FromSqlRow;
use reqwest::Url;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fmt::Debug};
use time::{macros::format_description, Date};
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use crate::{config::Config, sync_client::SyncClient};

#[derive(Deserialize)]
struct LastModifiedStruct {
    table: StackString,
    last_modified: DateTimeWrapper,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ImdbEpisodes {
    pub show: StackString,
    pub title: StackString,
    pub season: i32,
    pub episode: i32,
    pub airdate: Option<Date>,
    pub rating: Option<Decimal>,
    pub eptitle: StackString,
    pub epurl: StackString,
    pub id: Uuid,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ImdbRatings {
    pub index: Uuid,
    pub show: StackString,
    pub title: Option<StackString>,
    pub link: StackString,
    pub rating: Option<f64>,
    pub istv: Option<bool>,
    pub source: Option<StackString>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct MovieCollectionRow {
    pub idx: Uuid,
    pub path: StackString,
    pub show: StackString,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct MovieQueueRow {
    pub idx: i32,
    pub collection_idx: Uuid,
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
    pub added_at: DateTimeWrapper,
    pub updated_at: Option<DateTimeWrapper>,
    pub last_modified: DateTimeWrapper,
    pub metadata_type: Option<StackString>,
    pub section_type: Option<StackString>,
    pub section_title: Option<StackString>,
    pub metadata_key: Option<StackString>,
}

#[derive(FromSqlRow, Default, Debug, Serialize, Deserialize)]
pub struct PlexFilename {
    pub metadata_key: StackString,
    pub filename: StackString,
    pub collection_id: Option<Uuid>,
    pub music_collection_id: Option<Uuid>,
}

#[derive(FromSqlRow, Default, Debug, Serialize, Deserialize)]
pub struct PlexMetadata {
    pub metadata_key: StackString,
    pub object_type: StackString,
    pub title: StackString,
    pub parent_key: Option<StackString>,
    pub grandparent_key: Option<StackString>,
    pub show: Option<StackString>,
}

#[derive(FromSqlRow, Debug, Serialize, Deserialize)]
pub struct MusicCollection {
    pub id: Uuid,
    pub path: StackString,
    pub artist: Option<StackString>,
    pub album: Option<StackString>,
    pub title: Option<StackString>,
    pub last_modified: DateTimeWrapper,
}

impl Default for MusicCollection {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            path: StackString::new(),
            artist: None,
            album: None,
            title: None,
            last_modified: DateTimeWrapper::now(),
        }
    }
}

pub struct MovieSync {
    client: SyncClient,
}

impl MovieSync {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: Config) -> Result<Self, Error> {
        Ok(Self {
            client: SyncClient::new(config, "/usr/bin/movie-queue-cli")?,
        })
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("list", "movie-sync").await?;
        let mut output = Vec::new();

        let from_url = self.client.get_url()?;
        let url = from_url.join("list/last_modified")?;
        debug!("{}", url);
        let last_modified0 = self.get_last_modified(&url).await?;
        let last_modified1 = Self::transform_last_modified(
            self.client.get_local("last_modified", None, None).await?,
        );

        debug!("{:?} {:?}", last_modified0, last_modified1);

        macro_rules! sync_single_table {
            ($table:expr, $js_prefix:expr, $T:ty) => {{
                let table = $table;
                let js_prefix = $js_prefix;
                debug!("{} {}", table, js_prefix);
                let now = DateTimeWrapper::now();
                let last_mod0 = last_modified0.get(table).unwrap_or_else(|| &now);
                let last_mod1 = last_modified1.get(table).unwrap_or_else(|| &now);
                let results = self
                    .run_single_sync::<$T>(table, *last_mod0, *last_mod1, js_prefix)
                    .await?;
                output.extend_from_slice(&results);
            }};
        }

        debug!("plex_event");
        sync_single_table!("plex_event", "events", PlexEvent);
        debug!("plex_filename");
        sync_single_table!("plex_filename", "filenames", PlexFilename);
        debug!("plex_metadata");
        sync_single_table!("plex_metadata", "entries", PlexMetadata);
        debug!("imdb_ratings");
        sync_single_table!("imdb_ratings", "shows", ImdbRatings);
        debug!("imdb_episodes");
        sync_single_table!("imdb_episodes", "episodes", ImdbEpisodes);
        debug!("movie_collection");
        sync_single_table!("movie_collection", "collection", MovieCollectionRow);
        debug!("movie_queue");
        sync_single_table!("movie_queue", "queue", MovieQueueRow);
        debug!("music_collection");
        sync_single_table!("music_collection", "entries", MusicCollection);

        self.client.shutdown().await?;

        Ok(output)
    }

    async fn get_last_modified(
        &self,
        url: &Url,
    ) -> Result<HashMap<StackString, DateTimeWrapper>, Error> {
        let last_modified: Vec<LastModifiedStruct> = self.client.get_remote(url).await?;
        let results = Self::transform_last_modified(last_modified);
        Ok(results)
    }

    fn transform_last_modified(
        data: Vec<LastModifiedStruct>,
    ) -> HashMap<StackString, DateTimeWrapper> {
        data.into_iter()
            .map(|entry| (entry.table, entry.last_modified))
            .collect()
    }

    async fn run_single_sync<T>(
        &self,
        table: &str,
        last_modified_remote: DateTimeWrapper,
        last_modified_local: DateTimeWrapper,
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
        last_modified_remote: DateTimeWrapper,
        last_modified_local: DateTimeWrapper,
        js_prefix: &str,
    ) -> Result<Vec<StackString>, Error>
    where
        T: Debug + Serialize + DeserializeOwned + Send + 'static,
    {
        let mut output = Vec::new();
        let url = endpoint.join(&format_sstr!("list/{table}"))?;
        let last_modified_str = last_modified_local
            .format(format_description!(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]Z"
            ))
            .unwrap_or_default();
        let params = &[("start_timestamp".into(), last_modified_str.into())];
        debug!("{}", url);
        output.push(url.as_str().into());
        debug!("get_remote {url} {params:?}");
        let remote_data: Vec<T> = self.client.get_remote_paginated(&url, params).await?;
        let local_data: Vec<T> = self
            .client
            .get_local(table, Some(last_modified_remote.into()), None)
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
        let s = MovieSync::new(config).unwrap();
        let result = s.run_sync().await.unwrap();
        debug!("{:?}", result);
        assert!(result.len() > 0);
    }
}
