#![allow(clippy::semicolon_if_nothing_returned)]

use sync_app_http::app::start_app;

#[tokio::main]
async fn main() {
    env_logger::init();
    start_app().await.unwrap();
}
