use structopt::StructOpt;
use url::Url;

use crate::file_sync::{FileSyncAction, FileSyncMode};

#[derive(StructOpt, Debug)]
pub struct SyncOpts {
    #[structopt(short = "m", long = "mode", parse(from_str))]
    mode: FileSyncMode,
    #[structopt(parse(try_from_str))]
    action: FileSyncAction,
    #[structopt(short = "u", long = "urls", parse(try_from_str))]
    urls: Vec<Url>,
}
