use actix_web::http::StatusCode;
use actix_web::web::{block, Data, Query};
use actix_web::HttpResponse;

use sync_app_lib::file_sync::FileSyncAction;

use super::app::AppState;
use super::errors::ServiceError as Error;
use super::logged_user::LoggedUser;
use super::requests::{
    GarminSyncRequest, HandleRequest, ListSyncCacheRequest, MovieSyncRequest,
    SyncEntryDeleteRequest, SyncRemoveRequest, SyncRequest,
};

fn form_http_response(body: String) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(body))
}

pub async fn sync_frontpage(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let clist = data.db.handle(ListSyncCacheRequest {}).await?;
    let clist: Vec<_> = clist
        .into_iter()
        .map(|v| {
            format!(
                r#"
        <input type="button" name="Rm" value="Rm" onclick="removeCacheEntry({id})">
        {src} {dst}
        <input type="button" name="Del" value="Del" onclick="deleteEntry('{src}', {id})">"#,
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
    data.db.handle(req).await?;
    form_http_response("finished".to_string())
}

pub async fn proc_all(_: LoggedUser, data: Data<AppState>) -> Result<HttpResponse, Error> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    data.db.handle(req).await?;
    form_http_response("finished".to_string())
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

pub async fn remove(
    query: Query<SyncRemoveRequest>,
    _: LoggedUser,
    data: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let query = query.into_inner();
    data.db.handle(query).await?;
    form_http_response("finished".to_string())
}
