use anyhow::{format_err, Error};
use async_google_apis_common as common;
use common::{
    yup_oauth2::{self, hyper, hyper_rustls, InstalledFlowAuthenticator},
    DownloadResult, TlsClient,
};
use crossbeam::atomic::AtomicCell;
use futures::future::try_join_all;
use itertools::Itertools;
use log::debug;
use maplit::{hashmap, hashset};
use mime::Mime;
use percent_encoding::percent_decode;
use stack_string::{format_sstr, StackString};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fmt::{self, Debug, Formatter},
    future::Future,
    path::{Path, PathBuf},
    string::ToString,
    sync::{Arc, LazyLock},
};
use stdout_channel::rate_limiter::RateLimiter;
use tokio::{
    fs::{self, create_dir_all},
    io::AsyncReadExt,
};
use url::Url;

use crate::{
    directory_info::DirectoryInfo,
    drive_v3_types::{
        Change, ChangesGetStartPageTokenParams, ChangesListParams, ChangesService, DriveParams,
        DriveParamsAlt, DriveScopes, File, FileList, FilesCreateParams, FilesDeleteParams,
        FilesExportParams, FilesGetParams, FilesListParams, FilesService, FilesUpdateParams,
    },
    exponential_retry,
};

fn https_client() -> TlsClient {
    let conn = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_only()
        .enable_http1()
        .build();
    hyper::Client::builder().build(conn)
}

static MIME_TYPES: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    hashmap! {
        "application/vnd.google-apps.document" => "application/vnd.oasis.opendocument.text",
        "application/vnd.google-apps.presentation" => "application/pdf",
        "application/vnd.google-apps.spreadsheet" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.google-apps.drawing" => "image/png",
        "application/vnd.google-apps.site" => "text/plain",
    }
});
static UNEXPORTABLE_MIME_TYPES: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    hashset! {
        "application/vnd.google-apps.form",
        "application/vnd.google-apps.map",
        "application/vnd.google-apps.folder",
    }
});

#[derive(Clone)]
pub struct GDriveInstance {
    files: Arc<FilesService>,
    changes: Arc<ChangesService>,
    page_size: i32,
    max_keys: Option<usize>,
    session_name: StackString,
    pub start_page_token_filename: PathBuf,
    pub start_page_token: Arc<AtomicCell<Option<usize>>>,
    rate_limit: RateLimiter,
}

impl Debug for GDriveInstance {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str("GDriveInstance")
    }
}

impl GDriveInstance {
    /// # Errors
    /// Return error if intialization fails
    pub async fn new(
        gdrive_token_path: &Path,
        gdrive_secret_file: &Path,
        session_name: &str,
    ) -> Result<Self, Error> {
        let fname = gdrive_token_path.join(format_sstr!("{session_name}_start_page_token"));
        debug!("{}", gdrive_secret_file.display());
        let https = https_client();
        let sec = yup_oauth2::read_application_secret(gdrive_secret_file).await?;

        let token_file = gdrive_token_path.join(format_sstr!("{session_name}.json"));

        let parent = gdrive_token_path;

        if !parent.exists() {
            create_dir_all(parent).await?;
        }

        debug!("{}", token_file.display());
        let auth = InstalledFlowAuthenticator::builder(
            sec,
            common::yup_oauth2::InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk(token_file)
        .hyper_client(https.clone())
        .build()
        .await?;
        let auth = Arc::new(auth);

        let scopes = vec![DriveScopes::Drive];

        let mut files = FilesService::new(https.clone(), auth.clone());
        files.set_scopes(scopes.clone());

        let mut changes = ChangesService::new(https, auth);
        changes.set_scopes(scopes);

        let start_page_token = Self::read_start_page_token(&fname).await?;

        Ok(Self {
            files: Arc::new(files),
            changes: Arc::new(changes),
            page_size: 400,
            max_keys: None,
            session_name: session_name.into(),
            start_page_token: Arc::new(AtomicCell::new(start_page_token)),
            start_page_token_filename: fname,
            rate_limit: RateLimiter::new(1000, 60000),
        })
    }

    #[must_use]
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    #[must_use]
    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    /// # Errors
    /// Return error if intialization fails
    pub async fn read_start_page_token_from_file(&self) -> Result<(), Error> {
        self.start_page_token
            .store(Self::read_start_page_token(&self.start_page_token_filename).await?);
        Ok(())
    }

    async fn get_filelist(
        &self,
        page_token: Option<&StackString>,
        get_folders: bool,
        parents: Option<&[StackString]>,
    ) -> Result<FileList, Error> {
        let fields = vec![
            "name",
            "id",
            "size",
            "mimeType",
            "owners",
            "parents",
            "trashed",
            "modifiedTime",
            "createdTime",
            "viewedByMeTime",
            "md5Checksum",
            "fileExtension",
            "webContentLink",
        ];
        let fields = format!("nextPageToken,files({})", fields.join(","));
        let p = DriveParams {
            fields: Some(fields),
            ..DriveParams::default()
        };
        debug!("page_size {}", self.page_size);
        let mut params = FilesListParams {
            drive_params: Some(p),
            corpora: Some("user".into()),
            spaces: Some("drive".into()),
            page_size: Some(self.page_size),
            page_token: page_token.map(Into::into),
            ..FilesListParams::default()
        };
        let mut query_chain: Vec<StackString> = Vec::new();
        if get_folders {
            query_chain.push(r"mimeType = 'application/vnd.google-apps.folder'".into());
        } else {
            query_chain.push(r"mimeType != 'application/vnd.google-apps.folder'".into());
        }
        if let Some(p) = parents {
            let q = p
                .iter()
                .map(|id| format_sstr!("'{id}' in parents"))
                .join(" or ");

            query_chain.push(format_sstr!("({q})"));
        }
        query_chain.push("trashed = false".into());
        let query = query_chain.join(" and ");
        debug!("query {query}",);
        params.q = Some(query);

        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            self.files.list(&params).await
        })
        .await
    }

    /// # Errors
    /// Return error if `get_filelist` fails
    pub async fn get_all_files(&self, get_folders: bool) -> Result<Vec<File>, Error> {
        let mut all_files = Vec::new();
        let mut page_token: Option<StackString> = None;
        loop {
            let filelist = self
                .get_filelist(page_token.as_ref(), get_folders, None)
                .await?;

            if let Some(files) = filelist.files {
                debug!("got files {}", files.len());
                all_files.extend(files);
            }

            page_token = filelist.next_page_token.map(Into::into);
            debug!("page_token {get_folders} {page_token:?}",);
            if page_token.is_none() {
                break;
            }

            if let Some(max_keys) = self.max_keys {
                if all_files.len() > max_keys {
                    all_files.resize_with(max_keys, Default::default);
                    break;
                }
            }
        }
        Ok(all_files)
    }

    /// # Errors
    /// Return error if `get_all_files` fails
    pub async fn get_all_file_info(
        &self,
        get_folders: bool,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<GDriveInfo>, Error> {
        let files = self.get_all_files(get_folders).await?;
        self.convert_file_list_to_gdrive_info(&files, directory_map)
            .await
    }

    /// # Errors
    /// Return error if `from_object` fails
    #[allow(clippy::manual_filter_map)]
    pub async fn convert_file_list_to_gdrive_info(
        &self,
        flist: &[File],
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<GDriveInfo>, Error> {
        let futures = flist
            .iter()
            .filter(|f| {
                if let Some(owners) = f.owners.as_ref() {
                    if owners.is_empty() {
                        return false;
                    }
                    if owners[0].me != Some(true) {
                        return false;
                    }
                } else {
                    return false;
                }
                if Self::is_unexportable(&f.mime_type) {
                    return false;
                }
                true
            })
            .map(|f| GDriveInfo::from_object(f, self, directory_map));
        try_join_all(futures).await
    }

    /// # Errors
    /// Return error if `get_filelist` fails
    pub async fn process_list_of_keys<T, U>(
        &self,
        parents: Option<&[StackString]>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(File) -> U,
        U: Future<Output = Result<(), Error>>,
    {
        let mut n_processed = 0;
        let mut page_token: Option<StackString> = None;
        loop {
            let mut filelist = self
                .get_filelist(page_token.as_ref(), false, parents)
                .await?;

            if let Some(files) = filelist.files.take() {
                for f in files {
                    callback(f).await?;
                    n_processed += 1;
                }
            }

            page_token = filelist.next_page_token.map(Into::into);
            if page_token.is_none() {
                break;
            }

            if let Some(max_keys) = self.max_keys {
                if n_processed > max_keys {
                    break;
                }
            }
        }
        Ok(())
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_file_metadata(&self, id: &str) -> Result<File, Error> {
        let p = DriveParams {
            alt: Some(DriveParamsAlt::Json),
            fields: Some("id,name,parents,mimeType,webContentLink".into()),
            ..DriveParams::default()
        };
        let params = FilesGetParams {
            drive_params: Some(p),
            file_id: id.into(),
            ..FilesGetParams::default()
        };
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            if let DownloadResult::Response(f) = self.files.get(&params).await?.do_it(None).await? {
                Ok(f)
            } else {
                Err(format_err!("Failed to get metadata"))
            }
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn create_directory(&self, directory: &Url, parentid: &str) -> Result<File, Error> {
        let directory_path = directory
            .to_file_path()
            .map_err(|e| format_err!("No file path {e:?}"))?;
        let directory_name = directory_path
            .file_name()
            .map(OsStr::to_string_lossy)
            .ok_or_else(|| format_err!("Failed to convert string"))?;
        let new_file = File {
            name: Some(directory_name.into_owned()),
            mime_type: Some("application/vnd.google-apps.folder".to_string()),
            parents: Some(vec![parentid.to_string()]),
            ..File::default()
        };
        let params = FilesCreateParams::default();
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            self.files.create(&params, &new_file).await
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn upload(&self, local: &Url, parentid: &str) -> Result<File, Error> {
        let file_path = local
            .to_file_path()
            .map_err(|e| format_err!("No file path {e:?}"))?;
        let file_obj = fs::File::open(&file_path).await?;
        let mime: Mime = "application/octet-stream"
            .parse()
            .map_err(|e| format_err!("bad mimetype {e:?}"))?;
        let new_file = File {
            name: file_path
                .as_path()
                .file_name()
                .and_then(OsStr::to_str)
                .map(ToString::to_string),
            parents: Some(vec![parentid.to_string()]),
            mime_type: Some(mime.to_string()),
            ..File::default()
        };

        let params = FilesCreateParams {
            ..FilesCreateParams::default()
        };

        self.rate_limit.acquire().await;
        let upload = self
            .files
            .create_resumable_upload(&params, &new_file)
            .await?;
        let resp = upload.upload_file(file_obj).await?;
        Ok(resp)
    }

    pub fn is_unexportable<T: AsRef<str>>(mime_type: &Option<T>) -> bool {
        mime_type
            .as_ref()
            .is_some_and(|mime| UNEXPORTABLE_MIME_TYPES.contains::<str>(mime.as_ref()))
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn export(&self, gdriveid: &str, local: &Path, mime_type: &str) -> Result<(), Error> {
        let params = FilesExportParams {
            file_id: gdriveid.into(),
            mime_type: mime_type.into(),
            ..FilesExportParams::default()
        };
        let mut outfile = fs::File::create(local).await?;

        self.rate_limit.acquire().await;
        self.files
            .export(&params)
            .await?
            .do_it(Some(&mut outfile))
            .await?;
        Ok(())
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn download<T>(
        &self,
        gdriveid: &str,
        local: &Path,
        mime_type: &Option<T>,
    ) -> Result<(), Error>
    where
        T: AsRef<str> + Debug,
    {
        if let Some(mime) = mime_type {
            if UNEXPORTABLE_MIME_TYPES.contains::<str>(mime.as_ref()) {
                return Err(format_err!(
                    "UNEXPORTABLE_FILE: The MIME type of this file is {mime:?}, which can not be \
                     exported from Drive. Web content link provided by Drive: {m:?}\n",
                    m = self
                        .get_file_metadata(gdriveid)
                        .await
                        .ok()
                        .map(|metadata| metadata.web_view_link)
                        .unwrap_or_default()
                ));
            }
        }

        let export_type: Option<&'static str> = mime_type
            .as_ref()
            .and_then(|t| MIME_TYPES.get::<str>(t.as_ref()))
            .copied();

        if let Some(t) = export_type {
            self.export(gdriveid, local, t).await
        } else {
            let p = DriveParams {
                alt: Some(DriveParamsAlt::Media),
                ..DriveParams::default()
            };
            let params = FilesGetParams {
                drive_params: Some(p),
                file_id: gdriveid.into(),
                supports_all_drives: Some(false),
                ..FilesGetParams::default()
            };
            let mut outfile = fs::File::create(&local).await?;

            self.rate_limit.acquire().await;
            if let DownloadResult::Downloaded = self
                .files
                .get(&params)
                .await?
                .do_it(Some(&mut outfile))
                .await?
            {
                Ok(())
            } else {
                Err(format_err!("Failed to download"))
            }
        }
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn move_to_trash(&self, id: &str) -> Result<(), Error> {
        let f = File {
            trashed: Some(true),
            ..File::default()
        };
        let params = FilesUpdateParams {
            file_id: id.into(),
            supports_all_drives: Some(false),
            ..FilesUpdateParams::default()
        };
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            self.files.update(&params, &f).await?;
            Ok(())
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn delete_permanently(&self, id: &str) -> Result<(), Error> {
        let params = FilesDeleteParams {
            file_id: id.into(),
            supports_all_drives: Some(false),
            ..FilesDeleteParams::default()
        };
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            self.files.delete(&params).await
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn move_to(&self, id: &str, parent: &str, new_name: &str) -> Result<(), Error> {
        let current_parents = self
            .get_file_metadata(id)
            .await?
            .parents
            .unwrap_or_else(|| vec![String::from("root")])
            .join(",");

        let file = File {
            name: Some(new_name.to_string()),
            ..File::default()
        };
        let params = FilesUpdateParams {
            file_id: id.into(),
            supports_all_drives: Some(false),
            remove_parents: Some(current_parents),
            add_parents: Some(parent.into()),
            ..FilesUpdateParams::default()
        };
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            self.files.update(&params, &file).await?;
            Ok(())
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_directory_map(
        &self,
    ) -> Result<(HashMap<StackString, DirectoryInfo>, Option<StackString>), Error> {
        let mut root_id: Option<StackString> = None;
        let mut dmap: HashMap<StackString, _> = self
            .get_all_files(true)
            .await?
            .into_iter()
            .filter_map(|d| {
                if let Some(owners) = d.owners.as_ref() {
                    if owners.is_empty() {
                        return None;
                    }
                    if owners[0].me != Some(true) {
                        return None;
                    }
                } else {
                    return None;
                }
                if let Some(gdriveid) = d.id.as_ref() {
                    if let Some(name) = d.name.as_ref() {
                        if let Some(parents) = d.parents.as_ref() {
                            if !parents.is_empty() {
                                return Some((
                                    gdriveid.into(),
                                    DirectoryInfo {
                                        directory_id: gdriveid.into(),
                                        directory_name: name.into(),
                                        parentid: Some(parents[0].clone().into()),
                                    },
                                ));
                            }
                        } else {
                            if root_id.is_none()
                                && d.name != Some("Chrome Syncable FileSystem".to_string())
                            {
                                root_id = Some(gdriveid.into());
                            }
                            return Some((
                                gdriveid.into(),
                                DirectoryInfo {
                                    directory_id: gdriveid.into(),
                                    directory_name: name.into(),
                                    parentid: None,
                                },
                            ));
                        }
                    }
                }
                None
            })
            .collect();
        let unmatched_parents: HashSet<_> = dmap
            .values()
            .filter_map(|v| {
                v.parentid.as_ref().and_then(|p| match dmap.get(p) {
                    Some(_) => None,
                    None => Some(p.to_string()),
                })
            })
            .collect();
        for parent in unmatched_parents {
            let d = self.get_file_metadata(&parent).await?;
            if let Some(gdriveid) = d.id.as_ref() {
                if let Some(name) = d.name.as_ref() {
                    let parents = d
                        .parents
                        .as_ref()
                        .and_then(|p| p.first().map(ToString::to_string));
                    if parents.is_none()
                        && root_id.is_none()
                        && d.name != Some("Chrome Syncable FileSystem".to_string())
                    {
                        root_id = Some(gdriveid.into());
                    }
                    let val = DirectoryInfo {
                        directory_id: gdriveid.into(),
                        directory_name: name.into(),
                        parentid: parents.map(Into::into),
                    };

                    dmap.entry(gdriveid.into()).or_insert(val);
                }
            }
        }
        Ok((dmap, root_id))
    }

    #[must_use]
    pub fn get_directory_name_map(
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> HashMap<StackString, Vec<DirectoryInfo>> {
        directory_map.values().fold(HashMap::new(), |mut h, m| {
            let key = m.directory_name.clone();
            let val = m.clone();
            h.entry(key).or_default().push(val);
            h
        })
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_export_path(
        &self,
        finfo: &File,
        dirmap: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Vec<StackString>, Error> {
        let mut fullpath = Vec::new();
        if let Some(name) = finfo.name.as_ref() {
            fullpath.push(name.clone().into());
        }
        let mut pid: Option<StackString> = finfo
            .parents
            .as_ref()
            .and_then(|parents| parents.first().map(|p| p.clone().into()));
        loop {
            pid = if let Some(pid_) = pid.as_ref() {
                if let Some(dinfo) = dirmap.get(pid_) {
                    fullpath.push(format_sstr!("{}/", dinfo.directory_name));
                    dinfo.parentid.clone()
                } else {
                    self.get_file_metadata(pid_)
                        .await
                        .ok()
                        .as_ref()
                        .and_then(|f| f.parents.as_ref())
                        .and_then(|v| {
                            if v.is_empty() {
                                None
                            } else {
                                Some(v[0].clone().into())
                            }
                        })
                }
            } else {
                None
            };
            if pid.is_none() {
                break;
            }
        }
        Ok(fullpath.into_iter().rev().collect())
    }

    /// # Errors
    /// Return error if api call fails
    pub fn get_parent_id(
        url: &Url,
        dir_name_map: &HashMap<StackString, Vec<DirectoryInfo>>,
    ) -> Result<Option<StackString>, Error> {
        let mut previous_parent_id: Option<StackString> = None;
        if let Some(segments) = url.path_segments() {
            for seg in segments {
                let name = percent_decode(seg.as_bytes())
                    .decode_utf8_lossy()
                    .into_owned();
                let mut matching_directory: Option<StackString> = None;
                if let Some(parents) = dir_name_map.get(name.as_str()) {
                    for parent in parents {
                        if previous_parent_id.is_none() {
                            previous_parent_id = Some(parent.directory_id.clone());
                            matching_directory = Some(parent.directory_id.clone());
                            break;
                        }
                        if parent.parentid.is_some() && parent.parentid == previous_parent_id {
                            matching_directory = Some(parent.directory_id.clone());
                        }
                    }
                }
                if matching_directory.is_some() {
                    previous_parent_id = matching_directory.clone();
                } else {
                    return Ok(previous_parent_id);
                }
            }
        }
        Ok(None)
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_start_page_token(&self) -> Result<usize, Error> {
        let params = ChangesGetStartPageTokenParams {
            ..ChangesGetStartPageTokenParams::default()
        };
        exponential_retry(|| async {
            self.rate_limit.acquire().await;
            if let Some(start_page_token) = self
                .changes
                .get_start_page_token(&params)
                .await?
                .start_page_token
            {
                Ok(start_page_token.parse()?)
            } else {
                Err(format_err!(
                    "Received OK response from drive but there is no startPageToken included."
                ))
            }
        })
        .await
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn store_start_page_token(&self, path: &Path) -> Result<(), Error> {
        if let Some(start_page_token) = self.start_page_token.load().as_ref() {
            let buf = StackString::from_display(start_page_token);
            fs::write(path, buf).await?;
        }
        Ok(())
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn read_start_page_token(path: &Path) -> Result<Option<usize>, Error> {
        if !path.exists() {
            return Ok(None);
        }
        let mut f = fs::File::open(path).await?;
        let mut buf = String::new();
        f.read_to_string(&mut buf).await?;
        let start_page_token = buf.parse()?;
        Ok(Some(start_page_token))
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn get_all_changes(&self) -> Result<Vec<Change>, Error> {
        if let Some(start_page_token) = self.start_page_token.load() {
            let mut start_page_token = start_page_token.to_string();

            let mut all_changes = Vec::new();

            let changes_fields = ["kind", "type", "time", "removed", "fileId"].join(",");
            let file_fields = [
                "name",
                "id",
                "size",
                "mimeType",
                "owners",
                "parents",
                "trashed",
                "modifiedTime",
                "createdTime",
                "viewedByMeTime",
                "md5Checksum",
                "fileExtension",
                "webContentLink",
            ]
            .join(",");
            let fields = format!(
                "kind,nextPageToken,newStartPageToken,changes({changes_fields},\
                 file({file_fields}))"
            );

            loop {
                let p = DriveParams {
                    fields: Some(fields.clone()),
                    ..DriveParams::default()
                };
                let params = ChangesListParams {
                    drive_params: Some(p),
                    page_token: start_page_token,
                    spaces: Some("drive".into()),
                    restrict_to_my_drive: Some(true),
                    include_removed: Some(true),
                    supports_all_drives: Some(false),
                    page_size: Some(self.page_size),
                    ..ChangesListParams::default()
                };
                self.rate_limit.acquire().await;
                let changelist = self.changes.list(&params).await?;

                if let Some(changes) = changelist.changes {
                    all_changes.extend(changes);
                } else {
                    debug!("Changelist does not contain any changes!");
                    break;
                }
                if changelist.new_start_page_token.is_some() {
                    break;
                }
                match changelist.next_page_token {
                    Some(token) => start_page_token = token,
                    None => break,
                }
            }

            Ok(all_changes)
        } else {
            Ok(Vec::new())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GDriveInfo {
    pub filename: StackString,
    pub filepath: PathBuf,
    pub urlname: Url,
    pub md5sum: Option<StackString>,
    pub sha1sum: Option<StackString>,
    pub filestat: (u32, u32),
    pub serviceid: StackString,
    pub servicesession: StackString,
}

impl GDriveInfo {
    /// # Errors
    /// Return error if api call fails
    pub async fn from_object(
        item: &File,
        gdrive: &GDriveInstance,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Self, Error> {
        let filename = item
            .name
            .as_ref()
            .ok_or_else(|| format_err!("No filename"))?;
        let md5sum = item.md5_checksum.as_ref().and_then(|x| x.parse().ok());
        let st_mtime = item
            .modified_time
            .as_ref()
            .ok_or_else(|| format_err!("No last modified"))?
            .unix_timestamp();
        let size: u32 = item.size.as_ref().and_then(|x| x.parse().ok()).unwrap_or(0);
        let serviceid = item.id.as_ref().ok_or_else(|| format_err!("No ID"))?.into();
        let servicesession = gdrive.session_name.parse()?;

        let export_path = gdrive.get_export_path(item, directory_map).await?;
        let filepath = export_path.iter().fold(PathBuf::new(), |mut p, e| {
            p.push(e.as_str());
            p
        });
        let urlname = format_sstr!("gdrive://{}/", gdrive.session_name);
        let urlname = Url::parse(&urlname)?;
        let urlname = export_path.iter().try_fold(urlname, |u, e| {
            if e.contains('#') || e.contains('?') {
                u.join(&e.replace('#', "%23").replace('?', "%3F"))
            } else {
                u.join(e)
            }
        })?;

        let finfo = Self {
            filename: filename.into(),
            filepath,
            urlname,
            md5sum,
            sha1sum: None,
            filestat: (st_mtime as u32, size),
            serviceid,
            servicesession,
        };
        if item.id == Some("1t4plcsKgXK_NB025K01yFLKwljaTeM3i".to_string()) {
            debug!("{item:?}, {finfo:?}",);
        }

        Ok(finfo)
    }

    /// # Errors
    /// Return error if api call fails
    pub async fn from_changes_object(
        item: Change,
        gdrive: &GDriveInstance,
        directory_map: &HashMap<StackString, DirectoryInfo>,
    ) -> Result<Self, Error> {
        let file = item.file.ok_or_else(|| format_err!("No file"))?;
        Self::from_object(&file, gdrive, directory_map).await
    }
}
