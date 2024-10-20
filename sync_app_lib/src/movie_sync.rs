#![allow(clippy::too_many_arguments)]

use anyhow::Error;
use core::hash::Hash;
use log::debug;
use postgres_query::FromSqlRow;
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fmt::Debug};
use time::{format_description::well_known::Rfc3339, Date, Duration, OffsetDateTime};
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use crate::{config::Config, sync_client::SyncClient};

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
    pub id: Uuid,
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

        let results = self
            .run_single_sync_activities(
                "list/plex_event",
                "events",
                "plex_event",
                |items: Vec<PlexEvent>| {
                    items
                        .into_iter()
                        .map(|e| (StackString::from_display(e.id), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/plex_filename",
                "filenames",
                "plex_filename",
                |items: Vec<PlexFilename>| {
                    items
                        .into_iter()
                        .map(|e| (e.metadata_key.clone(), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/plex_metadata",
                "entries",
                "plex_metadata",
                |items: Vec<PlexMetadata>| {
                    items
                        .into_iter()
                        .map(|e| (e.metadata_key.clone(), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/imdb_ratings",
                "shows",
                "imdb_ratings",
                |items: Vec<ImdbRatings>| items.into_iter().map(|e| (e.show.clone(), e)).collect(),
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/imdb_episodes",
                "episodes",
                "imdb_episodes",
                |items: Vec<ImdbEpisodes>| {
                    items
                        .into_iter()
                        .map(|e| ((e.show.clone(), e.season, e.episode), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/movie_collection",
                "collection",
                "movie_collection",
                |items: Vec<MovieCollectionRow>| {
                    items.into_iter().map(|e| (e.path.clone(), e)).collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/movie_queue",
                "queue",
                "movie_queue",
                |items: Vec<MovieQueueRow>| {
                    items
                        .into_iter()
                        .map(|e| (StackString::from_display(e.collection_idx), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);
        let results = self
            .run_single_sync_activities(
                "list/music_collection",
                "entries",
                "music_collection",
                |items: Vec<MusicCollection>| {
                    items
                        .into_iter()
                        .map(|e| (StackString::from_display(&e.path), e))
                        .collect()
                },
            )
            .await?;
        output.extend_from_slice(&results);

        self.client.shutdown().await?;

        Ok(output)
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

        let start_timestamp = OffsetDateTime::now_utc() - Duration::days(7);
        let timetstamp_str = start_timestamp.format(&Rfc3339)?;
        let params = &[("start_timestamp".into(), timetstamp_str.into())];

        let url = from_url.join(path)?;
        debug!("url {url} params {params:?}");
        let activities0 = transform(self.client.get_remote_paginated(&url, params).await?);
        let activities1 = transform(
            self.client
                .get_local(table, Some(start_timestamp), None)
                .await?,
        );

        debug!(
            "activities0 {} activities1 {}",
            activities0.len(),
            activities1.len()
        );

        let activities2 = Self::combine_activities(&activities0, &activities1);
        let activities3 = Self::combine_activities(&activities1, &activities0);

        debug!(
            "activities2 {} activities3 {}",
            activities2.len(),
            activities3.len()
        );

        output.extend(Self::get_debug(table, &activities2));
        output.extend(Self::get_debug(table, &activities3));

        let url = from_url.join(path)?;
        debug!("put local {table}");
        self.client.put_local(table, &activities2, None).await?;
        debug!("put remote {url}");
        self.client
            .put_remote(&url, &activities3, js_prefix)
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
