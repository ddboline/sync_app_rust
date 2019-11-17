#[macro_use]
extern crate diesel;
#[macro_use]
extern crate serde_derive;

pub mod config;
pub mod directory_info;
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
pub mod gdrive_instance;
pub mod models;
pub mod pgpool;
pub mod s3_instance;
pub mod schema;
pub mod ssh_instance;
pub mod sync_opts;

use failure::{err_msg, format_err, Error};
use log::error;
use retry::{delay::jitter, delay::Exponential, retry};
use std::fmt;
use std::str::FromStr;

pub fn map_parse<T>(x: &Option<String>) -> Result<Option<T>, Error>
where
    T: FromStr,
    <T as std::str::FromStr>::Err: 'static + Send + Sync + fmt::Debug + fmt::Display,
{
    match x {
        Some(y) => Ok(Some(y.parse::<T>().map_err(err_msg)?)),
        None => Ok(None),
    }
}

pub fn exponential_retry<T, U>(closure: T) -> Result<U, Error>
where
    T: Fn() -> Result<U, Error>,
{
    retry(
        Exponential::from_millis(2)
            .map(jitter)
            .map(|x| x * 500)
            .take(6),
        || {
            closure().map_err(|e| {
                error!("Got error {:?} , retrying", e);
                e
            })
        },
    )
    .map_err(|e| format_err!("{:?}", e))
}
