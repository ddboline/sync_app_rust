use sync_app_rust::sync_opts::SyncOpts;

fn main() {
    env_logger::init();
    SyncOpts::process_args().unwrap();
}
