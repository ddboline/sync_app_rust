use actix_web::http::StatusCode;
use actix_web::web::{Data, Query};
use actix_web::HttpResponse;
use failure::Error;
use futures::Future;

use sync_app_lib::file_sync::FileSyncAction;

use super::app::AppState;
use super::logged_user::LoggedUser;
use super::requests::{ListSyncCacheRequest, SyncEntryDeleteRequest, SyncRequest};

fn form_http_response(body: String) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::build(StatusCode::OK)
        .content_type("text/html; charset=utf-8")
        .body(body))
}

pub fn sync_frontpage(
    _: LoggedUser,
    data: Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    data.db
        .send(ListSyncCacheRequest {})
        .from_err()
        .and_then(move |res| {
            res.and_then(|clist| {
                let clist: Vec<_> = clist
                    .into_iter()
                    .map(|v| {
                        format!(
                            r#"
                    <input type="button" name="{id}" value="{id}" onclick="removeCacheEntry({id})">
                    {src} {dst}"#,
                            id = v.id,
                            src = v.src_url,
                            dst = v.dst_url
                        )
                    })
                    .collect();
                let body = clist.join("<br>");
                form_http_response(
                    include_str!("../../templates/index.html").replace("DISPLAY_TEXT", &body),
                )
            })
        })
}

pub fn sync_all(
    _: LoggedUser,
    data: Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
    };
    data.db
        .send(req)
        .from_err()
        .and_then(move |res| res.and_then(|_| form_http_response("finished".to_string())))
}

pub fn proc_all(
    _: LoggedUser,
    data: Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let req = SyncRequest {
        action: FileSyncAction::Process,
    };
    data.db
        .send(req)
        .from_err()
        .and_then(move |res| res.and_then(|_| form_http_response("finished".to_string())))
}

pub fn list_sync_cache(
    _: LoggedUser,
    data: Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    data.db
        .send(ListSyncCacheRequest {})
        .from_err()
        .and_then(move |res| {
            res.and_then(|clist| {
                let clist: Vec<_> = clist
                    .into_iter()
                    .map(|v| format!("{} {}", v.src_url, v.dst_url))
                    .collect();
                let body = clist.join("\n");
                form_http_response(body)
            })
        })
}

pub fn delete_cache_entry(
    query: Query<SyncEntryDeleteRequest>,
    _: LoggedUser,
    data: Data<AppState>,
) -> impl Future<Item = HttpResponse, Error = Error> {
    let query = query.into_inner();
    data.db
        .send(query)
        .from_err()
        .and_then(move |res| res.and_then(|_| form_http_response("finished".to_string())))
}
