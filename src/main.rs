use sync_app_rust::sync_opts::SyncOpts;

fn main() {
    SyncOpts::process_args().unwrap();
}
