use axum::http::{header::CONTENT_TYPE, Method, StatusCode};
use deadqueue::unlimited::Queue;
use log::{debug, error};
use reqwest::{Client, ClientBuilder};
use stack_string::format_sstr;
use std::{net::SocketAddr, sync::Arc, time};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle, time::interval};
use tower_http::cors::{Any, CorsLayer};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;

use sync_app_lib::{
    calendar_sync::CalendarSync, config::Config, garmin_sync::GarminSync, movie_sync::MovieSync,
    pgpool::PgPool, security_sync::SecuritySync, sync_opts::SyncOpts, weather_sync::WeatherSync,
};

use super::{
    errors::ServiceError as Error,
    logged_user::{fill_from_db, get_secrets, SyncMesg},
    routes::{get_sync_path, ApiDoc},
};

pub struct AccessLocks {
    pub sync: Mutex<SyncOpts>,
    pub garmin: Mutex<GarminSync>,
    pub movie: Mutex<MovieSync>,
    pub calendar: Mutex<CalendarSync>,
    pub podcast: Mutex<()>,
    pub security: Mutex<SecuritySync>,
    pub weather: Mutex<WeatherSync>,
}

impl AccessLocks {
    /// # Errors
    /// Returns error if creation of client fails
    pub fn new(config: &Config) -> Result<Self, Error> {
        Ok(Self {
            sync: Mutex::new(SyncOpts::default()),
            garmin: Mutex::new(GarminSync::new(config.clone())?),
            movie: Mutex::new(MovieSync::new(config.clone())?),
            calendar: Mutex::new(CalendarSync::new(config.clone())?),
            podcast: Mutex::new(()),
            security: Mutex::new(SecuritySync::new(config.clone())?),
            weather: Mutex::new(WeatherSync::new(config.clone())?),
        })
    }
}

type SyncJob = (SyncMesg, JoinHandle<Result<(), Error>>);

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: PgPool,
    pub locks: Arc<AccessLocks>,
    pub client: Arc<Client>,
    pub queue: Arc<Queue<SyncJob>>,
}

/// # Errors
/// Return error if app init fails
pub async fn start_app() -> Result<(), Error> {
    async fn update_db(pool: PgPool) {
        let mut i = interval(time::Duration::from_secs(60));
        loop {
            fill_from_db(&pool).await.unwrap_or(());
            i.tick().await;
        }
    }

    let config = Config::init_config()?;
    get_secrets(&config.secret_path, &config.jwt_secret_path).await?;
    let pool = PgPool::new(&config.database_url)?;

    tokio::task::spawn(update_db(pool.clone()));
    let port = config.port;

    run_app(config, port, pool).await
}

async fn run_app(config: Config, port: u32, pool: PgPool) -> Result<(), Error> {
    async fn run_queue(app: AppState) {
        loop {
            let (SyncMesg { user, key }, task) = app.queue.pop().await;
            match task.await {
                Ok(Err(e)) => {
                    error!("Failure running job {e}",);
                    if let Err(e) = user
                        .rm_session(&app.client, &app.config, key.to_str())
                        .await
                    {
                        error!("Failed to delete session {e}",);
                    }
                }
                Err(e) => error!("join error {e}",),
                _ => {}
            }
        }
    }

    let locks = Arc::new(AccessLocks::new(&config)?);
    let client = Arc::new(ClientBuilder::new().build()?);
    let queue = Arc::new(Queue::new());

    let app = AppState {
        config,
        db: pool,
        locks,
        client,
        queue,
    };

    let queue_task = tokio::task::spawn(run_queue(app.clone()));

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([CONTENT_TYPE])
        .allow_origin(Any);

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .merge(get_sync_path(&app))
        .layer(cors)
        .split_for_parts();

    let spec_json = serde_json::to_string_pretty(&api)?;
    let spec_yaml = serde_yaml_ng::to_string(&api)?;

    let router = router
        .route(
            "/sync/openapi/json",
            axum::routing::get(|| async move {
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, mime::APPLICATION_JSON.essence_str())],
                    spec_json,
                )
            }),
        )
        .route(
            "/sync/openapi/yaml",
            axum::routing::get(|| async move {
                (StatusCode::OK, [(CONTENT_TYPE, "text/yaml")], spec_yaml)
            }),
        );

    let addr: SocketAddr = format_sstr!("127.0.0.1:{port}").parse()?;
    debug!("{addr:?}");
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, router.into_make_service()).await?;
    queue_task.await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use anyhow::Error;
    use stack_string::format_sstr;

    use crate::app::run_app;
    use sync_app_lib::{config::Config, pgpool::PgPool};

    #[tokio::test]
    async fn test_run_app() -> Result<(), Error> {
        let config = Config::init_config()?;
        let pool = PgPool::new(&config.database_url)?;

        let test_port = 12345;
        tokio::task::spawn({
            async move {
                env_logger::init();
                run_app(config, test_port, pool).await.unwrap()
            }
        });
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let client = reqwest::Client::new();

        let url = format_sstr!("http://localhost:{test_port}/sync/openapi/yaml");
        let spec_yaml = client
            .get(url.as_str())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        tokio::fs::write("../scripts/openapi.yaml", &spec_yaml).await?;

        Ok(())
    }
}
