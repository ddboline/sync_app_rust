use itertools::Itertools;
use rweb::{get, post, Json, Query, Rejection, Reply};
use rweb_helper::{
    html_response::HtmlResponse as HtmlBase, json_response::JsonResponse as JsonBase, RwebResponse,
};
use serde::Serialize;
use stack_string::StackString;
use std::time::Duration;
use tokio::time::sleep;

use sync_app_lib::file_sync::FileSyncAction;

use super::{
    app::{AppState, SyncMesg},
    errors::ServiceError as Error,
    logged_user::{LoggedUser, SyncKey},
    requests::{
        CalendarSyncRequest, GarminSyncRequest, ListSyncCacheRequest, MovieSyncRequest,
        SyncEntryDeleteRequest, SyncEntryProcessRequest, SyncPodcastsRequest, SyncRemoveRequest,
        SyncRequest, SyncSecurityRequest,
    },
};

pub type WarpResult<T> = Result<T, Rejection>;
pub type HttpResult<T> = Result<T, Error>;

#[derive(RwebResponse)]
#[response(description = "Main Page")]
struct IndexResponse(HtmlBase<String, Error>);

#[get("/sync/index.html")]
pub async fn sync_frontpage(
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<IndexResponse> {
    let body = ListSyncCacheRequest {}
        .handle(&data.db)
        .await?
        .into_iter()
        .map(|v| {
            format!(
                r#"
        <input type="button" name="Rm" value="Rm" onclick="removeCacheEntry({id})">
        <input type="button" name="DelSrc" value="DelSrc" onclick="deleteEntry('{src}', {id})">
        {src} {dst}
        <input type="button" name="DelDst" value="DelDst" onclick="deleteEntry('{dst}', {id})">
        <input type="button" name="Proc" value="Proc" onclick="procCacheEntry({id})">"#,
                id = v.id,
                src = v.src_url,
                dst = v.dst_url
            )
        })
        .join("<br>");
    let body = include_str!("../../templates/index.html").replace("DISPLAY_TEXT", &body);
    Ok(HtmlBase::new(body).into())
}

#[derive(RwebResponse)]
#[response(description = "Sync")]
struct SyncResponse(HtmlBase<String, Error>);

#[get("/sync/sync")]
pub async fn sync_all(
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncResponse> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
    };
    let result = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(result.join("\n")).into())
}

#[derive(RwebResponse)]
#[response(description = "Process All")]
struct ProcAllResponse(HtmlBase<String, Error>);

#[get("/sync/proc_all")]
pub async fn proc_all(
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<ProcAllResponse> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    let lines = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(lines.join("\n")).into())
}

#[derive(RwebResponse)]
#[response(description = "List Sync Cache")]
struct ListSyncCacheResponse(HtmlBase<String, Error>);

#[get("/sync/list_sync_cache")]
pub async fn list_sync_cache(
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<ListSyncCacheResponse> {
    let body = ListSyncCacheRequest {}
        .handle(&data.db)
        .await?
        .into_iter()
        .map(|v| format!("{} {}", v.src_url, v.dst_url))
        .join("\n");
    Ok(HtmlBase::new(body).into())
}

#[derive(RwebResponse)]
#[response(description = "Process Entry")]
struct ProcessEntryResponse(HtmlBase<&'static str, Error>);

#[get("/sync/proc")]
pub async fn process_cache_entry(
    query: Query<SyncEntryProcessRequest>,
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<ProcessEntryResponse> {
    query
        .into_inner()
        .handle(&data.locks, &data.db, &data.config)
        .await?;
    Ok(HtmlBase::new("Finished").into())
}

#[derive(RwebResponse)]
#[response(description = "Delete Cache Entry")]
struct DeleteEntryResponse(HtmlBase<&'static str, Error>);

#[get("/sync/delete_cache_entry")]
pub async fn delete_cache_entry(
    query: Query<SyncEntryDeleteRequest>,
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<DeleteEntryResponse> {
    query.into_inner().handle(&data.db).await?;
    Ok(HtmlBase::new("Finished").into())
}

#[derive(RwebResponse)]
#[response(description = "Sync Garmin DB")]
struct SyncGarminResponse(HtmlBase<String, Error>);

#[get("/sync/sync_garmin")]
pub async fn sync_garmin(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncGarminResponse> {
    match user.push_session(SyncKey::SyncGarmin, data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(RwebResponse)]
#[response(description = "Sync Movie DB")]
struct SyncMovieResponse(HtmlBase<String, Error>);

#[get("/sync/sync_movie")]
pub async fn sync_movie(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncMovieResponse> {
    match user.push_session(SyncKey::SyncMovie, data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(RwebResponse)]
#[response(description = "Sync Calendar DB")]
struct SyncCalendarResponse(HtmlBase<String, Error>);

#[get("/sync/sync_calendar")]
pub async fn sync_calendar(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncCalendarResponse> {
    match user.push_session(SyncKey::SyncCalendar, data).await? {
        Some(result) => Ok(HtmlBase::new(result.join("<br>")).into()),
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(RwebResponse)]
#[response(description = "Remove Sync Entry")]
struct SyncRemoveResponse(HtmlBase<String, Error>);

#[get("/sync/remove")]
pub async fn remove(
    query: Query<SyncRemoveRequest>,
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncRemoveResponse> {
    let lines = query
        .into_inner()
        .handle(&data.locks, &data.config, &data.db)
        .await?;
    Ok(HtmlBase::new(lines.join("\n")).into())
}

#[derive(RwebResponse)]
#[response(description = "Sync Podcasts")]
struct SyncPodcastsResponse(HtmlBase<String, Error>);

#[get("/sync/sync_podcasts")]
pub async fn sync_podcasts(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncPodcastsResponse> {
    match user.push_session(SyncKey::SyncPodcast, data).await? {
        Some(result) => {
            let body = format!(
                r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
                result.join("\n")
            );
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}

#[derive(RwebResponse)]
#[response(description = "Logged in User")]
struct UserResponse(JsonBase<LoggedUser, Error>);

#[get("/sync/user")]
pub async fn user(#[filter = "LoggedUser::filter"] user: LoggedUser) -> WarpResult<UserResponse> {
    Ok(JsonBase::new(user).into())
}

#[derive(RwebResponse)]
#[response(description = "Sync Security Logs")]
struct SyncSecurityLogsResponse(HtmlBase<String, Error>);

#[get("/sync/sync_security")]
pub async fn sync_security(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncSecurityLogsResponse> {
    match user.push_session(SyncKey::SyncSecurity, data).await? {
        Some(result) => {
            let body = format!(
                r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
                result.join("\n")
            );
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}
