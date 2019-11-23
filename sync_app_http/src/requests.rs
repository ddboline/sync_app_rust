use actix::{Handler, Message};
use failure::Error;
use serde::{Deserialize, Serialize};

use sync_app_lib::config::Config;
use sync_app_lib::file_sync::FileSyncAction;
use sync_app_lib::garmin_sync::GarminSync;
use sync_app_lib::models::FileSyncCache;
use sync_app_lib::movie_sync::MovieSync;
use sync_app_lib::pgpool::PgPool;
use sync_app_lib::sync_opts::SyncOpts;

pub struct SyncRequest {
    pub action: FileSyncAction,
}

impl Message for SyncRequest {
    type Result = Result<(), Error>;
}

impl Handler<SyncRequest> for PgPool {
    type Result = Result<(), Error>;
    fn handle(&mut self, req: SyncRequest, _: &mut Self::Context) -> Self::Result {
        let config = Config::init_config()?;
        let opts = SyncOpts::new(req.action, &[]);
        opts.process_sync_opts(&config, self)
    }
}

pub struct ListSyncCacheRequest {}

impl Message for ListSyncCacheRequest {
    type Result = Result<Vec<FileSyncCache>, Error>;
}

impl Handler<ListSyncCacheRequest> for PgPool {
    type Result = Result<Vec<FileSyncCache>, Error>;
    fn handle(&mut self, _: ListSyncCacheRequest, _: &mut Self::Context) -> Self::Result {
        FileSyncCache::get_cache_list(self)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncEntryDeleteRequest {
    pub id: i32,
}

impl Message for SyncEntryDeleteRequest {
    type Result = Result<(), Error>;
}

impl Handler<SyncEntryDeleteRequest> for PgPool {
    type Result = Result<(), Error>;
    fn handle(&mut self, req: SyncEntryDeleteRequest, _: &mut Self::Context) -> Self::Result {
        FileSyncCache::delete_by_id(self, req.id)
    }
}

pub struct GarminSyncRequest {}

impl Message for GarminSyncRequest {
    type Result = Result<Vec<String>, Error>;
}

impl Handler<GarminSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    fn handle(&mut self, _: GarminSyncRequest, _: &mut Self::Context) -> Self::Result {
        let config = Config::init_config()?;
        let sync = GarminSync::new(config);
        sync.run_sync()
    }
}

pub struct MovieSyncRequest {}

impl Message for MovieSyncRequest {
    type Result = Result<Vec<String>, Error>;
}

impl Handler<MovieSyncRequest> for PgPool {
    type Result = Result<Vec<String>, Error>;
    fn handle(&mut self, _: MovieSyncRequest, _: &mut Self::Context) -> Self::Result {
        let config = Config::init_config()?;
        let sync = MovieSync::new(config);
        sync.run_sync()
    }
}
