use actix::sync::SyncArbiter;
use actix::Addr;
use actix_identity::{CookieIdentityPolicy, IdentityService};
use actix_web::{web, App, HttpServer};
use chrono::Duration;
use futures::future::Future;
use futures::stream::Stream;
use std::time;
use tokio_timer::Interval;

use sync_app_lib::config::Config;
use sync_app_lib::pgpool::PgPool;

use super::logged_user::AUTHORIZED_USERS;
use super::routes::{
    delete_cache_entry, list_sync_cache, proc_all, sync_all, sync_frontpage, sync_garmin,
};

pub struct AppState {
    pub db: Addr<PgPool>,
}

pub fn start_app() {
    let config = Config::init_config().expect("Failed to load config");
    let pool = PgPool::new(&config.database_url);

    let _p = pool.clone();

    actix_rt::spawn(
        Interval::new(time::Instant::now(), time::Duration::from_secs(60))
            .for_each(move |_| {
                AUTHORIZED_USERS.fill_from_db(&_p).unwrap_or(());
                Ok(())
            })
            .map_err(|e| panic!("error {:?}", e)),
    );

    let addr: Addr<PgPool> = SyncArbiter::start(config.n_db_workers, move || pool.clone());

    let port = config.port;

    HttpServer::new(move || {
        App::new()
            .data(AppState { db: addr.clone() })
            .wrap(IdentityService::new(
                CookieIdentityPolicy::new(config.secret_key.as_bytes())
                    .name("auth")
                    .path("/")
                    .domain(config.domain.as_str())
                    .max_age_time(Duration::days(1))
                    .secure(false),
            ))
            .service(web::resource("/sync/index.html").route(web::get().to_async(sync_frontpage)))
            .service(web::resource("/sync/sync").route(web::get().to_async(sync_all)))
            .service(web::resource("/sync/proc").route(web::get().to_async(proc_all)))
            .service(
                web::resource("/sync/list_sync_cache").route(web::get().to_async(list_sync_cache)),
            )
            .service(
                web::resource("/sync/delete_cache_entry")
                    .route(web::get().to_async(delete_cache_entry)),
            )
            .service(web::resource("/sync/sync_garmin").route(web::get().to_async(sync_garmin)))
    })
    .bind(&format!("127.0.0.1:{}", port))
    .unwrap_or_else(|_| panic!("Failed to bind to port {}", port))
    .start();
}
