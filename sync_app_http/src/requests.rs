use failure::Error;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use sync_app_lib::config::Config;
use sync_app_lib::file_sync::FileSyncAction;
use sync_app_lib::garmin_sync::GarminSync;
use sync_app_lib::models::FileSyncCache;
use sync_app_lib::movie_sync::MovieSync;
use sync_app_lib::pgpool::PgPool;
use sync_app_lib::sync_opts::SyncOpts;

lazy_static! {
    static ref CONFIG: Config = Config::init_config().expect("Failed to load config");
}

pub trait HandleRequest<T> {
    type Result;
    fn handle(&self, req: T) -> Self::Result;
}

pub struct SyncRequest {
    pub action: FileSyncAction,
}

impl HandleRequest<SyncRequest> for PgPool {
    type Result = Result<(), Error>;
    fn handle(&self, req: SyncRequest) -> Self::Result {
        let opts = SyncOpts::new(req.action, &[]);
        opts.process_sync_opts(&CONFIG, self)
    }
}

pub struct ListSyncCacheRequest {}

impl HandleRequest<ListSyncCacheRequest> for PgPool {
    type Result = Result<Vec<FileSyncCache>, Error>;
    fn handle(&self, _: ListSyncCacheRequest) -> Self::Result {
        FileSyncCache::get_cache_list(self)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncEntryDeleteRequest {
    pub id: i32,
}

impl HandleRequest<SyncEntryDeleteRequest> for PgPool {
    type Result = Result<(), Error>;
    fn handle(&self, req: SyncEntryDeleteRequest) -> Self::Result {
        FileSyncCache::delete_by_id(self, req.id)
    }
}

pub struct GarminSyncRequest {}

impl HandleRequest<GarminSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    fn handle(&self, _: GarminSyncRequest) -> Self::Result {
        let config = CONFIG.clone();
        let sync = GarminSync::new(config);
        sync.run_sync()
    }
}

pub struct MovieSyncRequest {}

impl HandleRequest<MovieSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    fn handle(&self, _: MovieSyncRequest) -> Self::Result {
        let config = CONFIG.clone();
        let sync = MovieSync::new(config);
        sync.run_sync()
    }
}

#[derive(Serialize, Deserialize)]
pub struct SyncRemoveRequest {
    pub url: String,
}

impl HandleRequest<SyncRemoveRequest> for PgPool {
    type Result = Result<(), Error>;
    fn handle(&self, req: SyncRemoveRequest) -> Self::Result {
        let url = req.url.parse()?;
        let sync = SyncOpts::new(FileSyncAction::Delete, &[url]);
        sync.process_sync_opts(&CONFIG, self)
    }
}
