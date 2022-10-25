use itertools::Itertools;
use maplit::hashmap;
use rweb::{get, Query, Rejection};
use rweb_helper::{
    html_response::HtmlResponse as HtmlBase, json_response::JsonResponse as JsonBase, RwebResponse,
};
use stack_string::{format_sstr, StackString};

use sync_app_lib::{
    file_sync::FileSyncAction,
    models::{FileSyncCache, FileSyncConfig},
};

use super::{
    app::AppState,
    errors::ServiceError as Error,
    logged_user::{LoggedUser, SyncKey},
    requests::{SyncEntryDeleteRequest, SyncEntryProcessRequest, SyncRemoveRequest, SyncRequest},
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
    let conf_list = FileSyncConfig::get_config_list(&data.db)
        .await.map_err(Into::<Error>::into)?.into_iter().map(|v| {
            if let Some(name) = v.name.as_ref() {
                format_sstr!(r#"
                    <input type="button" type="submit" name="sync-{name}" value="{name}" onclick="syncName( '{name}' )">
                "#)
            } else {
                "".into()
            }
        }).join("<br>");
    let body = FileSyncCache::get_cache_list(&data.db)
        .await
        .map_err(Into::<Error>::into)?
        .into_iter()
        .map(|v| {
            format_sstr!(
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
    let params = hashmap! {
        "LIST_TEXT" => conf_list,
        "DISPLAY_TEXT" => body,
    };
    let body = data.hb.render("id", &params).map_err(Into::<Error>::into)?;
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
        name: None,
    };
    let result = req.process(&data.db, &data.config, &data.locks).await?;
    Ok(HtmlBase::new(result.join("\n")).into())
}

#[get("/sync/sync/{name}")]
pub async fn sync_name(
    #[filter = "LoggedUser::filter"] _: LoggedUser,
    #[data] data: AppState,
    name: StackString,
) -> WarpResult<SyncResponse> {
    let req = SyncRequest {
        action: FileSyncAction::Sync,
        name: Some(name),
    };
    let result = req.process(&data.db, &data.config, &data.locks).await?;
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
        name: None,
    };
    let lines = req.process(&data.db, &data.config, &data.locks).await?;
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
    let body = FileSyncCache::get_cache_list(&data.db)
        .await
        .map_err(Into::<Error>::into)?
        .into_iter()
        .map(|v| format_sstr!("{} {}", v.src_url, v.dst_url))
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
        .process(&data.locks, &data.db, &data.config)
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
    let query = query.into_inner();
    FileSyncCache::delete_by_id(&data.db, query.id.into())
        .await
        .map_err(Into::<Error>::into)?;
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
        .process(&data.locks, &data.config, &data.db)
        .await?;
    Ok(HtmlBase::new(lines.join("\n")).into())
}

#[derive(RwebResponse)]
#[response(description = "Sync Podcasts")]
struct SyncPodcastsResponse(HtmlBase<StackString, Error>);

#[get("/sync/sync_podcasts")]
pub async fn sync_podcasts(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncPodcastsResponse> {
    match user.push_session(SyncKey::SyncPodcast, data).await? {
        Some(result) => {
            let body = format_sstr!(
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
struct SyncSecurityLogsResponse(HtmlBase<StackString, Error>);

#[get("/sync/sync_security")]
pub async fn sync_security(
    #[filter = "LoggedUser::filter"] user: LoggedUser,
    #[data] data: AppState,
) -> WarpResult<SyncSecurityLogsResponse> {
    match user.push_session(SyncKey::SyncSecurity, data).await? {
        Some(result) => {
            let body = format_sstr!(
                r#"<textarea autofocus readonly="readonly" rows=50 cols=100>{}</textarea>"#,
                result.join("\n")
            );
            Ok(HtmlBase::new(body).into())
        }
        None => Ok(HtmlBase::new("running".into()).into()),
    }
}
