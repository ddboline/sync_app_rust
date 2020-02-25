use actix_identity::{CookieIdentityPolicy, IdentityService};
use actix_web::{web, App, HttpServer};
use chrono::Duration;
use std::time;
use tokio::time::interval;

use sync_app_lib::config::Config;
use sync_app_lib::pgpool::PgPool;

use super::logged_user::{fill_from_db, TRIGGER_DB_UPDATE};
use super::routes::{
    delete_cache_entry, list_sync_cache, proc_all, remove, sync_all, sync_frontpage, sync_garmin,
    sync_movie,
};

pub struct AppState {
    pub db: PgPool,
}

pub async fn start_app() {
    async fn _update_db(pool: PgPool) {
        let mut i = interval(time::Duration::from_secs(60));
        loop {
            i.tick().await;
            fill_from_db(&pool).await.unwrap_or(());
        }
    }
    TRIGGER_DB_UPDATE.set();

    let config = Config::init_config().expect("Failed to load config");
    let pool = PgPool::new(&config.database_url);

    actix_rt::spawn(_update_db(pool.clone()));

    let port = config.port;

    HttpServer::new(move || {
        App::new()
            .data(AppState { db: pool.clone() })
            .wrap(IdentityService::new(
                CookieIdentityPolicy::new(config.secret_key.as_bytes())
                    .name("auth")
                    .path("/")
                    .domain(config.domain.as_str())
                    .max_age_time(Duration::days(1))
                    .secure(false),
            ))
            .service(web::resource("/sync/index.html").route(web::get().to(sync_frontpage)))
            .service(web::resource("/sync/sync").route(web::get().to(sync_all)))
            .service(web::resource("/sync/proc").route(web::get().to(proc_all)))
            .service(web::resource("/sync/remove").route(web::get().to(remove)))
            .service(web::resource("/sync/list_sync_cache").route(web::get().to(list_sync_cache)))
            .service(
                web::resource("/sync/delete_cache_entry").route(web::get().to(delete_cache_entry)),
            )
            .service(web::resource("/sync/sync_garmin").route(web::get().to(sync_garmin)))
            .service(web::resource("/sync/sync_movie").route(web::get().to(sync_movie)))
    })
    .bind(&format!("127.0.0.1:{}", port))
    .unwrap_or_else(|_| panic!("Failed to bind to port {}", port))
    .run()
    .await
    .expect("Failed to start app");
}
