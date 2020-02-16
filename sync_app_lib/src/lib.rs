#![allow(unused_imports)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::similar_names)]
#![allow(clippy::shadow_unrelated)]
#![allow(clippy::missing_errors_doc)]

#[macro_use]
extern crate diesel;

pub mod config;
pub mod file_info;
pub mod file_info_gdrive;
pub mod file_info_local;
pub mod file_info_s3;
pub mod file_info_ssh;
pub mod file_list;
pub mod file_list_gdrive;
pub mod file_list_local;
pub mod file_list_s3;
pub mod file_list_ssh;
pub mod file_service;
pub mod file_sync;
pub mod garmin_sync;
pub mod iso_8601_datetime;
pub mod models;
pub mod movie_sync;
pub mod path_buf_wrapper;
pub mod pgpool;
pub mod reqwest_session;
pub mod s3_instance;
pub mod schema;
pub mod ssh_instance;
pub mod sync_opts;
pub mod url_wrapper;

use anyhow::Error;
use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::thread_rng;
use std::future::Future;
use std::str::FromStr;
use tokio::time::{delay_for, Duration};

pub fn map_parse<T>(x: &Option<String>) -> Result<Option<T>, Error>
where
    T: FromStr,
    <T as std::str::FromStr>::Err: Into<Error>,
{
    x.as_ref()
        .map(|y| y.parse::<T>())
        .transpose()
        .map_err(Into::into)
}

pub async fn exponential_retry<T, U, F>(f: T) -> Result<U, Error>
where
    T: Fn() -> F,
    F: Future<Output = Result<U, Error>>,
{
    let mut timeout: f64 = 1.0;
    let range = Uniform::from(0..1000);
    loop {
        match f().await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                delay_for(Duration::from_millis((timeout * 1000.0) as u64)).await;
                timeout *= 4.0 * f64::from(range.sample(&mut thread_rng())) / 1000.0;
                if timeout >= 64.0 {
                    return Err(err.into());
                }
            }
        }
    }
}
