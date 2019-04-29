#[macro_use]
extern crate diesel;

pub mod file_info;
pub mod file_info_local;
pub mod file_list;
pub mod file_list_local;
pub mod file_service;
pub mod models;
pub mod pgpool;
pub mod schema;

use failure::{err_msg, Error};

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
