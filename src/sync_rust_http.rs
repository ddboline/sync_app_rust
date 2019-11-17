use sync_app_http::app::start_app;

fn main() {
    env_logger::init();
    let sys = actix_rt::System::new("sync_app_http");
    start_app();
    let _ = sys.run();
}
