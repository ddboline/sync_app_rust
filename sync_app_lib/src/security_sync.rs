use anyhow::Error;
use log::{debug, error};
use postgres_query::FromSqlRow;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use stack_string::{format_sstr, StackString};
use std::{collections::HashMap, fmt, fmt::Debug, hash::Hash};
use uuid::Uuid;

use gdrive_lib::date_time_wrapper::DateTimeWrapper;

use crate::{config::Config, sync_client::SyncClient};

#[derive(FromSqlRow, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IntrusionLog {
    pub id: Uuid,
    pub service: StackString,
    pub server: StackString,
    pub datetime: DateTimeWrapper,
    pub host: StackString,
    pub username: Option<StackString>,
}

impl fmt::Display for IntrusionLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}",
            self.service, self.server, self.datetime, self.host
        )
    }
}

#[derive(FromSqlRow, Clone, Debug, Serialize, Deserialize)]
pub struct HostCountry {
    pub host: StackString,
    pub code: StackString,
    pub ipaddr: Option<StackString>,
    pub created_at: DateTimeWrapper,
}

pub struct SecuritySync {
    client: SyncClient,
}

impl SecuritySync {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: Config) -> Result<Self, Error> {
        Ok(Self {
            client: SyncClient::new(config, "/usr/bin/security-log-parse-rust")?,
        })
    }

    /// # Errors
    /// Return error if db query fails
    pub async fn run_sync(&self) -> Result<Vec<StackString>, Error> {
        self.client.init("security_log", "security-sync").await?;

        let mut output = Vec::new();

        let results = match self
            .run_single_sync(
                "security_log/intrusion_log",
                "updates",
                "intrusion_log",
                |results: Vec<IntrusionLog>| {
                    debug!("intrusion_log {}", results.len());
                    results
                        .into_iter()
                        .map(|val| {
                            let key = format_sstr!("{val}");
                            (key, val)
                        })
                        .collect()
                },
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                error!("Recieved error, shutting down");
                self.client.shutdown().await?;
                return Err(e);
            }
        };
        output.extend_from_slice(&results);

        let url = self.client.get_url()?;
        let url = url.join("security_log/cleanup")?;
        let remote_hosts: Vec<HostCountry> = match self.client.get_remote(&url).await {
            Ok(x) => x,
            Err(e) => {
                error!("Recieved error, shutting down");
                self.client.shutdown().await?;
                return Err(e);
            }
        };
        output.extend(remote_hosts.into_iter().map(|h| format_sstr!("{h:?}")));
        let local_hosts: Result<Vec<HostCountry>, _> =
            self.client.get_local_command(&["cleanup"]).await;
        if let Ok(local_hosts) = local_hosts {
            output.extend(local_hosts.into_iter().map(|h| format_sstr!("{h:?}")));
        }
        self.client.shutdown().await?;
        Ok(output)
    }

    async fn run_single_sync<T, U, V>(
        &self,
        path: &str,
        js_prefix: &str,
        table: &str,
        mut transform: T,
    ) -> Result<Vec<StackString>, Error>
    where
        T: FnMut(Vec<U>) -> HashMap<V, U>,
        U: DeserializeOwned + Send + 'static + Debug + Serialize,
        V: Hash + Eq,
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

    fn combine_maps<'a, T, U>(
        measurements0: &'a HashMap<U, T>,
        measurements1: &'a HashMap<U, T>,
    ) -> Vec<&'a T>
    where
        U: Hash + Eq,
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
}
