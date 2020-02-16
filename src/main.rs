use anyhow::Error;
use sync_app_lib::sync_opts::SyncOpts;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    SyncOpts::process_args().await
}
