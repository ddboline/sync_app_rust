#[macro_use]
extern crate diesel;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate maplit;

pub mod config;
pub mod directory_info;
pub mod file_info;
pub mod file_info_gdrive;
pub mod file_info_local;
pub mod file_info_s3;
pub mod file_list;
pub mod file_list_gdrive;
pub mod file_list_local;
pub mod file_list_s3;
pub mod file_service;
pub mod file_sync;
pub mod gdrive_instance;
pub mod models;
pub mod onedrive_instance;
pub mod pgpool;
pub mod s3_instance;
pub mod schema;
pub mod sync_opts;

use failure::{err_msg, Error};
use rand::distributions::{Distribution, Uniform};
use rand::thread_rng;
use std::fmt;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

pub fn map_result_vec<T>(input: Vec<Result<T, Error>>) -> Result<Vec<T>, Error> {
    let mut output: Vec<T> = Vec::new();
    let mut errors: Vec<_> = Vec::new();
    for item in input {
        match item {
            Ok(i) => output.push(i),
            Err(e) => errors.push(format!("{}", e)),
        }
    }
    if !errors.is_empty() {
        Err(err_msg(errors.join("\n")))
    } else {
        Ok(output)
    }
}

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
    let mut timeout: f64 = 1.0;
    let mut rng = thread_rng();
    let range = Uniform::from(0..1000);
    loop {
        match closure() {
            Ok(x) => return Ok(x),
            Err(e) => {
                sleep(Duration::from_millis((timeout * 1000.0) as u64));
                timeout *= 4.0 * f64::from(range.sample(&mut rng)) / 1000.0;
                if timeout >= 64.0 {
                    return Err(err_msg(e));
                }
            }
        }
    }
}
