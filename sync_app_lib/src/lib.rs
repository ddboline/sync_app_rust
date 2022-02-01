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
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::missing_panics_doc)]

pub mod calendar_sync;
pub mod config;
pub mod file_info;
pub mod file_info_gcs;
pub mod file_info_gdrive;
pub mod file_info_local;
pub mod file_info_s3;
pub mod file_info_ssh;
pub mod file_list;
pub mod file_list_gcs;
pub mod file_list_gdrive;
pub mod file_list_local;
pub mod file_list_s3;
pub mod file_list_ssh;
pub mod file_service;
pub mod file_sync;
pub mod garmin_sync;
pub mod iso_8601_datetime;
pub mod local_session;
pub mod models;
pub mod movie_sync;
pub mod path_buf_wrapper;
pub mod pgpool;
pub mod reqwest_session;
pub mod s3_instance;
pub mod security_sync;
pub mod ssh_instance;
pub mod sync_client;
pub mod sync_opts;
pub mod url_wrapper;

use anyhow::Error;
use rand::{
    distributions::{Alphanumeric, Distribution, Uniform},
    thread_rng,
};
use std::{future::Future, str::FromStr};
use tokio::time::{sleep, Duration};

pub fn map_parse<T, U>(x: &Option<U>) -> Result<Option<T>, Error>
where
    T: FromStr,
    <T as std::str::FromStr>::Err: Into<Error>,
    U: AsRef<str>,
{
    x.as_ref()
        .map(|y| y.as_ref().parse::<T>())
        .transpose()
        .map_err(Into::into)
}
