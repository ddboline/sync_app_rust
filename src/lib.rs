#[macro_use]
extern crate diesel;

pub mod config;
pub mod file_info;
pub mod file_info_local;
pub mod file_info_s3;
pub mod file_list;
pub mod file_list_local;
pub mod file_list_s3;
pub mod file_service;
pub mod file_sync;
pub mod models;
pub mod pgpool;
pub mod s3_instance;
pub mod schema;
pub mod sync_opts;

use failure::{err_msg, Error};
use std::fmt;
use std::str::FromStr;

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

pub fn map_parse<T>(x: Option<String>) -> Result<Option<T>, Error>
where
    T: FromStr,
    <T as std::str::FromStr>::Err: 'static + Send + Sync + fmt::Debug + fmt::Display,
{
    match x {
        Some(y) => Ok(Some(y.parse::<T>().map_err(err_msg)?)),
        None => Ok(None),
    }
}
