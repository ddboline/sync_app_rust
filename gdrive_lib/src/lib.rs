#![allow(clippy::too_many_lines)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::empty_docs)]
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::assigning_clones)]

pub mod date_time_wrapper;
pub mod directory_info;
pub mod drive_v3_types;
pub mod gcs_instance;
pub mod gdrive_instance;
pub mod storage_v1_types;

use anyhow::Error;
use rand::{
    distr::{Distribution, Uniform},
    rng as thread_rng,
};
use std::{convert::TryFrom, future::Future};
use tokio::time::{sleep, Duration};

/// # Errors
/// Returns error if timeout is reached
pub async fn exponential_retry<T, U, F>(f: T) -> Result<U, Error>
where
    T: Fn() -> F,
    F: Future<Output = Result<U, Error>>,
{
    let mut timeout: f64 = 1.0;
    let range = Uniform::try_from(0..1000)?;
    loop {
        match f().await {
            Ok(resp) => return Ok(resp),
            Err(err) => {
                sleep(Duration::from_millis((timeout * 1000.0) as u64)).await;
                timeout *= 4.0 * f64::from(range.sample(&mut thread_rng())) / 1000.0;
                if timeout >= 64.0 {
                    return Err(err);
                }
            }
        }
    }
}
