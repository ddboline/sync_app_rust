use itertools::Itertools;
use rweb::{get, post, Json, Query, Rejection, Reply};
use serde::Serialize;

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

#[get("/sync/index.html")]
pub async fn sync_frontpage(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
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
    Ok(rweb::reply::html(body))
}

#[get("/sync/sync")]
pub async fn sync_all(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
    };
    let lines = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(rweb::reply::html(lines.join("\n")))
}

#[get("/sync/proc_all")]
pub async fn proc_all(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    let lines = req.handle(&data.db, &data.config, &data.locks).await?;
    Ok(rweb::reply::html(lines.join("\n")))
}

#[get("/sync/list_sync_cache")]
pub async fn list_sync_cache(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let body = ListSyncCacheRequest {}
        .handle(&data.db)
        .await?
        .into_iter()
        .map(|v| format!("{} {}", v.src_url, v.dst_url))
        .join("\n");
    Ok(rweb::reply::html(body))
}

#[get("/sync/proc")]
pub async fn process_cache_entry(
    query: Query<SyncEntryProcessRequest>,
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    query
        .into_inner()
        .handle(&data.locks, &data.db, &data.config)
        .await?;
    Ok(rweb::reply::html("finished"))
}

#[get("/sync/delete_cache_entry")]
pub async fn delete_cache_entry(
    query: Query<SyncEntryDeleteRequest>,
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    query.into_inner().handle(&data.db).await?;
    Ok(rweb::reply::html("finished"))
}

#[get("/sync/sync_garmin")]
pub async fn sync_garmin(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let body = GarminSyncRequest {}.handle(&data.locks).await?;
    Ok(rweb::reply::html(body.join("<br>")))
}

#[get("/sync/sync_movie")]
pub async fn sync_movie(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let body = MovieSyncRequest {}.handle(&data.locks).await?;
    Ok(rweb::reply::html(body.join("<br>")))
}

#[get("/sync/sync_calendar")]
pub async fn sync_calendar(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let body = CalendarSyncRequest {}.handle(&data.locks).await?;
    Ok(rweb::reply::html(body.join("<br>")))
}

#[get("/sync/remove")]
pub async fn remove(
    query: Query<SyncRemoveRequest>,
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let lines = query
        .into_inner()
        .handle(&data.locks, &data.config, &data.db)
        .await?;
    Ok(rweb::reply::html(lines.join("\n")))
}

#[get("/sync/sync_podcasts")]
pub async fn sync_podcasts(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let results = SyncPodcastsRequest {}.handle(&data.locks).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    Ok(rweb::reply::html(body))
}

#[get("/sync/user")]
pub async fn user(#[cookie = "jwt"] user: LoggedUser) -> WarpResult<impl Reply> {
    Ok(rweb::reply::json(&user))
}

#[get("/sync/sync_security")]
pub async fn sync_security(
    #[cookie = "jwt"] _: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<impl Reply> {
    let results = SyncSecurityRequest {}.handle(&data.locks).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    Ok(rweb::reply::html(body))
}
