use actix_identity::{CookieIdentityPolicy, IdentityService};
use actix_web::{web, App, HttpServer, middleware::Compress};
use anyhow::Error;
use chrono::Duration;
use std::time;
use tokio::time::interval;

use sync_app_lib::{config::Config, pgpool::PgPool};

use super::{
    logged_user::{fill_from_db, get_secrets, SECRET_KEY, TRIGGER_DB_UPDATE},
    routes::{
        delete_cache_entry, list_sync_cache, proc_all, process_cache_entry, remove, sync_all,
        sync_calendar, sync_frontpage, sync_garmin, sync_movie, sync_podcasts, sync_security, user,
    },
};

pub struct AppState {
    pub db: PgPool,
}

pub async fn start_app() -> Result<(), Error> {
    async fn _update_db(pool: PgPool) {
        let mut i = interval(time::Duration::from_secs(60));
        loop {
            fill_from_db(&pool).await.unwrap_or(());
            i.tick().await;
        }
    }
    TRIGGER_DB_UPDATE.set();

    let config = Config::init_config().expect("Failed to load config");
    get_secrets(&config.secret_path, &config.jwt_secret_path).await?;
    let pool = PgPool::new(&config.database_url);

    actix_rt::spawn(_update_db(pool.clone()));

    let port = config.port;

    HttpServer::new(move || {
        App::new()
            .data(AppState { db: pool.clone() })
            .wrap(Compress::default())
            .wrap(IdentityService::new(
                CookieIdentityPolicy::new(&SECRET_KEY.get())
                    .name("auth")
                    .path("/")
                    .domain(config.domain.as_str())
                    .max_age(24 * 3600)
                    .secure(false),
            ))
            .service(
                web::scope("/sync")
                    .service(web::resource("/index.html").route(web::get().to(sync_frontpage)))
                    .service(web::resource("/sync").route(web::get().to(sync_all)))
                    .service(web::resource("/proc_all").route(web::get().to(proc_all)))
                    .service(web::resource("/proc").route(web::get().to(process_cache_entry)))
                    .service(web::resource("/remove").route(web::get().to(remove)))
                    .service(
                        web::resource("/list_sync_cache").route(web::get().to(list_sync_cache)),
                    )
                    .service(
                        web::resource("/delete_cache_entry")
                            .route(web::get().to(delete_cache_entry)),
                    )
                    .service(web::resource("/sync_garmin").route(web::get().to(sync_garmin)))
                    .service(web::resource("/sync_movie").route(web::get().to(sync_movie)))
                    .service(web::resource("/sync_calendar").route(web::get().to(sync_calendar)))
                    .service(web::resource("/sync_podcasts").route(web::get().to(sync_podcasts)))
                    .service(web::resource("/sync_security").route(web::get().to(sync_security)))
                    .service(web::resource("/user").route(web::get().to(user))),
            )
    })
    .bind(&format!("127.0.0.1:{}", port))
    .unwrap_or_else(|_| panic!("Failed to bind to port {}", port))
    .run()
    .await
    .map_err(Into::into)
}
