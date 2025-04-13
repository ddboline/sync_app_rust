use axum::extract::{Path, Query, State};
use futures::TryStreamExt;
use stack_string::{format_sstr, StackString};
use std::sync::Arc;
use utoipa::{OpenApi, PartialSchema};
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_helper::{
    html_response::HtmlResponse as HtmlBase, json_response::JsonResponse as JsonBase,
    UtoipaResponse,
};

use sync_app_lib::{
    file_sync::FileSyncAction,
    models::{FileSyncCache, FileSyncConfig},
};

use super::{
    app::AppState,
    elements::{index_body, text_body},
    errors::ServiceError as Error,
    logged_user::{LoggedUser, SyncKey},
    requests::{SyncEntryDeleteRequest, SyncEntryProcessRequest, SyncRemoveRequest, SyncRequest},
};

type WarpResult<T> = Result<T, Error>;

#[derive(UtoipaResponse)]
#[response(description = "Main Page")]
#[rustfmt::skip]
struct IndexResponse(HtmlBase::<String>);

#[utoipa::path(get, path = "/sync/index.html", responses(IndexResponse, Error))]
async fn sync_frontpage(_: LoggedUser, data: State<Arc<AppState>>) -> WarpResult<IndexResponse> {
    let conf_list: Vec<FileSyncConfig> = FileSyncConfig::get_config_list(&data.db)
        .await
        .map_err(Into::<Error>::into)?
        .try_collect()
        .await
        .map_err(Into::<Error>::into)?;
    let entries: Vec<FileSyncCache> = FileSyncCache::get_cache_list(&data.db)
        .await
        .map_err(Into::<Error>::into)?
        .try_collect()
        .await
        .map_err(Into::<Error>::into)?;
    let body = index_body(conf_list, entries)?;
    Ok(HtmlBase::new(body).into())
}

#[derive(UtoipaResponse)]
#[response(description = "Javascript", content = "text/javascript")]
#[rustfmt::skip]
struct JsResponse(HtmlBase::<&'static str>);

#[utoipa::path(get, path = "/sync/scripts.js", responses(JsResponse))]
async fn garmin_scripts_js() -> JsResponse {
    HtmlBase::new(include_str!("../../templates/scripts.js")).into()
}

#[derive(UtoipaResponse)]
#[response(description = "Sync")]
#[rustfmt::skip]
struct SyncResponse(HtmlBase::<String>);

#[utoipa::path(post, path = "/sync/sync", responses(SyncResponse, Error))]
async fn sync_all(_: LoggedUser, data: State<Arc<AppState>>) -> WarpResult<SyncResponse> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
        name: None,
    };
    let result = req.process(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(result.join("\n")).into())
}

#[utoipa::path(post, path = "/sync/sync/{name}", params(("name" = inline(StackString), description = "Name")), responses(SyncResponse, Error))]
async fn sync_name(
    _: LoggedUser,
    data: State<Arc<AppState>>,
    name: Path<StackString>,
) -> WarpResult<SyncResponse> {
    let Path(name) = name;
    let req = SyncRequest {
        action: FileSyncAction::Sync,
        name: Some(name),
    };
    let result = req.process(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(result.join("\n")).into())
}

#[derive(UtoipaResponse)]
#[response(description = "Process All")]
#[rustfmt::skip]
struct ProcAllResponse(HtmlBase::<String>);

#[utoipa::path(post, path = "/sync/proc_all", responses(ProcAllResponse, Error))]
async fn proc_all(_: LoggedUser, data: State<Arc<AppState>>) -> WarpResult<ProcAllResponse> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
        name: None,
    };
    let lines = req.process(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(lines.join("\n")).into())
}

#[derive(UtoipaResponse)]
#[response(description = "List Sync Cache")]
#[rustfmt::skip]
struct ListSyncCacheResponse(HtmlBase::<String>);

#[utoipa::path(
    get,
    path = "/sync/list_sync_cache",
    responses(ListSyncCacheResponse, Error)
)]
async fn list_sync_cache(
    _: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<ListSyncCacheResponse> {
    let entries: Vec<_> = FileSyncCache::get_cache_list(&data.db)
        .await
        .map_err(Into::<Error>::into)?
        .map_ok(|v| format_sstr!("{} {}", v.src_url, v.dst_url))
        .try_collect()
        .await
        .map_err(Into::<Error>::into)?;
    let body = entries.join("\n");
    Ok(HtmlBase::new(body).into())
}

#[derive(UtoipaResponse)]
#[response(description = "Process Entry")]
#[rustfmt::skip]
struct ProcessEntryResponse(HtmlBase::<&'static str>);

#[utoipa::path(
    post,
    path = "/sync/proc",
    params(SyncEntryProcessRequest),
    responses(ProcessEntryResponse, Error)
)]
async fn process_cache_entry(
    query: Query<SyncEntryProcessRequest>,
    _: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<ProcessEntryResponse> {
    let Query(query) = query;
    query.process(&data.locks, &data.db, &data.config).await?;
    Ok(HtmlBase::new("Finished").into())
}

#[derive(UtoipaResponse)]
#[response(description = "Delete Cache Entry")]
#[rustfmt::skip]
struct DeleteEntryResponse(HtmlBase::<&'static str>);

#[utoipa::path(
    delete,
    path = "/sync/delete_cache_entry",
    params(SyncEntryDeleteRequest),
    responses(DeleteEntryResponse, Error)
)]
async fn delete_cache_entry(
    query: Query<SyncEntryDeleteRequest>,
    _: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<DeleteEntryResponse> {
    let Query(query) = query;
    FileSyncCache::delete_by_id(&data.db, query.id)
        .await
        .map_err(Into::<Error>::into)?;
    Ok(HtmlBase::new("Finished").into())
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Garmin DB")]
#[rustfmt::skip]
struct SyncGarminResponse(HtmlBase::<String>);

#[utoipa::path(post, path = "/sync/sync_garmin", responses(SyncGarminResponse, Error))]
async fn sync_garmin(
    user: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncGarminResponse> {
    match user.push_session(SyncKey::SyncGarmin, &data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Movie DB")]
#[rustfmt::skip]
struct SyncMovieResponse(HtmlBase::<String>);

#[utoipa::path(post, path = "/sync/sync_movie", responses(SyncMovieResponse, Error))]
async fn sync_movie(user: LoggedUser, data: State<Arc<AppState>>) -> WarpResult<SyncMovieResponse> {
    match user.push_session(SyncKey::SyncMovie, &data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Calendar DB")]
#[rustfmt::skip]
struct SyncCalendarResponse(HtmlBase::<String>);

#[utoipa::path(
    post,
    path = "/sync/sync_calendar",
    responses(SyncCalendarResponse, Error)
)]
async fn sync_calendar(
    user: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncCalendarResponse> {
    match user.push_session(SyncKey::SyncCalendar, &data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(UtoipaResponse)]
#[response(description = "Remove Sync Entry")]
#[rustfmt::skip]
struct SyncRemoveResponse(HtmlBase::<String>);

#[utoipa::path(
    delete,
    path = "/sync/remove",
    params(SyncRemoveRequest),
    responses(SyncRemoveResponse, Error)
)]
async fn remove(
    query: Query<SyncRemoveRequest>,
    _: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncRemoveResponse> {
    let Query(query) = query;
    let lines = query.process(&data.locks, &data.config, &data.db).await?;
    Ok(HtmlBase::new(lines.join("\n")).into())
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Podcasts")]
#[rustfmt::skip]
struct SyncPodcastsResponse(HtmlBase::<StackString>);

#[utoipa::path(
    post,
    path = "/sync/sync_podcasts",
    responses(SyncPodcastsResponse, Error)
)]
async fn sync_podcasts(
    user: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncPodcastsResponse> {
    match user.push_session(SyncKey::SyncPodcast, &data).await? {
        Some(result) => {
            let body = text_body(result.join("\n").into())?.into();
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(UtoipaResponse)]
#[response(description = "Logged in User")]
#[rustfmt::skip]
struct UserResponse(JsonBase::<LoggedUser>);

#[utoipa::path(get, path = "/sync/user", responses(UserResponse))]
async fn user(user: LoggedUser) -> UserResponse {
    JsonBase::new(user).into()
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Security Logs")]
#[rustfmt::skip]
struct SyncSecurityLogsResponse(HtmlBase::<StackString>);

#[utoipa::path(
    post,
    path = "/sync/sync_security",
    responses(SyncSecurityLogsResponse, Error)
)]
async fn sync_security(
    user: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncSecurityLogsResponse> {
    match user.push_session(SyncKey::SyncSecurity, &data).await? {
        Some(result) => {
            let body = text_body(result.join("\n").into())?.into();
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(UtoipaResponse)]
#[response(description = "Sync Weather Data")]
#[rustfmt::skip]
struct SyncWeatherDataResponse(HtmlBase::<StackString>);

#[utoipa::path(
    post,
    path = "/sync/sync_weather",
    responses(SyncWeatherDataResponse, Error)
)]
async fn sync_weather(
    user: LoggedUser,
    data: State<Arc<AppState>>,
) -> WarpResult<SyncWeatherDataResponse> {
    match user.push_session(SyncKey::SyncWeather, &data).await? {
        Some(result) => {
            let body = text_body(result.join("\n").into())?.into();
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

pub fn get_sync_path(app: &AppState) -> OpenApiRouter {
    let app = Arc::new(app.clone());

    OpenApiRouter::new()
        .routes(routes!(sync_frontpage))
        .routes(routes!(garmin_scripts_js))
        .routes(routes!(sync_all))
        .routes(routes!(sync_name))
        .routes(routes!(proc_all))
        .routes(routes!(process_cache_entry))
        .routes(routes!(remove))
        .routes(routes!(list_sync_cache))
        .routes(routes!(delete_cache_entry))
        .routes(routes!(sync_garmin))
        .routes(routes!(sync_movie))
        .routes(routes!(sync_calendar))
        .routes(routes!(sync_podcasts))
        .routes(routes!(sync_security))
        .routes(routes!(sync_weather))
        .routes(routes!(user))
        .with_state(app)
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "File Sync WebApp",
        description = "Web Frontend for File Sync Service",
    ),
    components(schemas(LoggedUser))
)]
pub struct ApiDoc;
