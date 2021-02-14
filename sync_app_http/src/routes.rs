use itertools::Itertools;
use serde::Serialize;
use warp::{Rejection, Reply};

use sync_app_lib::file_sync::FileSyncAction;

use super::{
    app::AppState,
    errors::ServiceError as Error,
    logged_user::LoggedUser,
    requests::{
        CalendarSyncRequest, GarminSyncRequest, ListSyncCacheRequest, MovieSyncRequest,
        SyncEntryDeleteRequest, SyncEntryProcessRequest, SyncPodcastsRequest, SyncRemoveRequest,
        SyncRequest, SyncSecurityRequest,
    },
};

pub type WarpResult<T> = Result<T, Rejection>;
pub type HttpResult<T> = Result<T, Error>;

pub async fn sync_frontpage(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
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
    Ok(warp::reply::html(body))
}

pub async fn sync_all(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
    };
    let lines = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(warp::reply::html(lines.join("\n")))
}

pub async fn proc_all(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    let lines = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(warp::reply::html(lines.join("\n")))
}

pub async fn list_sync_cache(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let body = ListSyncCacheRequest {}
        .handle(&data.db)
        .await?
        .into_iter()
        .map(|v| format!("{} {}", v.src_url, v.dst_url))
        .join("\n");
    Ok(warp::reply::html(body))
}

pub async fn process_cache_entry(
    query: SyncEntryProcessRequest,
    _: LoggedUser,
    data: AppState,
) -> WarpResult<impl Reply> {
    query.handle(&data.locks, &data.db, &data.config).await?;
    Ok(warp::reply::html("finished"))
}

pub async fn delete_cache_entry(
    query: SyncEntryDeleteRequest,
    _: LoggedUser,
    data: AppState,
) -> WarpResult<impl Reply> {
    query.handle(&data.db).await?;
    Ok(warp::reply::html("finished"))
}

pub async fn sync_garmin(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let body = GarminSyncRequest {}.handle(&data.locks).await?;
    Ok(warp::reply::html(body.join("<br>")))
}

pub async fn sync_movie(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let body = MovieSyncRequest {}.handle(&data.locks).await?;
    Ok(warp::reply::html(body.join("<br>")))
}

pub async fn sync_calendar(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let body = CalendarSyncRequest {}.handle(&data.locks).await?;
    Ok(warp::reply::html(body.join("<br>")))
}

pub async fn remove(
    query: SyncRemoveRequest,
    _: LoggedUser,
    data: AppState,
) -> WarpResult<impl Reply> {
    let lines = query.handle(&data.locks, &data.config, &data.db).await?;
    Ok(warp::reply::html(lines.join("\n")))
}

pub async fn sync_podcasts(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let results = SyncPodcastsRequest {}.handle(&data.locks).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    Ok(warp::reply::html(body))
}

pub async fn user(user: LoggedUser) -> WarpResult<impl Reply> {
    Ok(warp::reply::json(&user))
}

pub async fn sync_security(_: LoggedUser, data: AppState) -> WarpResult<impl Reply> {
    let results = SyncSecurityRequest {}.handle(&data.locks).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    Ok(warp::reply::html(body))
}
