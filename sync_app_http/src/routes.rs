use actix_web::{
    http::StatusCode,
    web::{block, Data, Query},
    HttpResponse,
};
use serde::Serialize;

use sync_app_lib::file_sync::FileSyncAction;

use super::{
    app::AppState,
    errors::ServiceError as Error,
    logged_user::LoggedUser,
    requests::{
        CalendarSyncRequest, GarminSyncRequest, HandleRequest, ListSyncCacheRequest,
        MovieSyncRequest, SyncEntryDeleteRequest, SyncEntryProcessRequest, SyncPodcastsRequest,
        SyncRemoveRequest, SyncRequest, SyncSecurityRequest,
    },
};

fn form_http_response(body: String) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(body))
}

fn to_json<T>(js: T) -> Result<HttpResponse, Error>
where
    T: Serialize,
{
    Ok(HttpResponse::Ok().json(js))
}

pub async fn sync_frontpage(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let clist = data.db.handle(ListSyncCacheRequest {}).await?;
    let clist: Vec<_> = clist
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
        .collect();
    let body = clist.join("<br>");
    form_http_response(include_str!("../../templates/index.html").replace("DISPLAY_TEXT", &body))
}

pub async fn sync_all(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
    };
    let lines = data.db.handle(req).await?;
    form_http_response(lines.join("\n"))
}

pub async fn proc_all(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    let lines = data.db.handle(req).await?;
    form_http_response(lines.join("\n"))
}

pub async fn list_sync_cache(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let clist = data.db.handle(ListSyncCacheRequest {}).await?;
    let clist: Vec<_> = clist
        .into_iter()
        .map(|v| format!("{} {}", v.src_url, v.dst_url))
        .collect();
    let body = clist.join("\n");
    form_http_response(body)
}

pub async fn process_cache_entry(
    query: Query<SyncEntryProcessRequest>,
    _: LoggedUser,
    data: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let query = query.into_inner();
    data.db.handle(query).await?;
    form_http_response("finished".to_string())
}

pub async fn delete_cache_entry(
    query: Query<SyncEntryDeleteRequest>,
    _: LoggedUser,
    data: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let query = query.into_inner();
    data.db.handle(query).await?;
    form_http_response("finished".to_string())
}

pub async fn sync_garmin(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let body = data.db.handle(GarminSyncRequest {}).await?;
    form_http_response(body.join("<br>"))
}

pub async fn sync_movie(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let body = data.db.handle(MovieSyncRequest {}).await?;
    form_http_response(body.join("<br>"))
}

pub async fn sync_calendar(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let body = data.db.handle(CalendarSyncRequest {}).await?;
    form_http_response(body.join("<br>"))
}

pub async fn remove(
    query: Query<SyncRemoveRequest>,
    _: LoggedUser,
    data: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let query = query.into_inner();
    let lines = data.db.handle(query).await?;
    form_http_response(lines.join("\n"))
}

pub async fn sync_podcasts(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let results = data.db.handle(SyncPodcastsRequest {}).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    form_http_response(body)
}

pub async fn user(user: LoggedUser) -> Result<HttpResponse, Error> {
    to_json(user)
}

pub async fn sync_security(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let results = data.db.handle(SyncSecurityRequest {}).await?;
    let body = format!(
        r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
        results.join("\n")
    );
    form_http_response(body)
}
