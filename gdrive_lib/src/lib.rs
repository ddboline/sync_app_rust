pub mod directory_info;
pub mod gdrive_instance;

use anyhow::{format_err, Error};
use log::error;
use retry::{delay::jitter, delay::Exponential, retry};

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
