use anyhow::{format_err, Error};
use std::path::Path;
use log::debug;
use std::sync::Arc;
use std::fmt::{self, Debug};
use async_google_apis_common as common;
use common::{
    yup_oauth2::{self, InstalledFlowAuthenticator},
    DownloadResult, TlsClient,
};
use tokio::{fs::{self, create_dir_all}};
use lazy_static::lazy_static;
use parking_lot::{Mutex, MutexGuard};
use stack_string::StackString;

use crate::storage_v1_types::{
    ObjectsService, ObjectsListParams,
    Object, StorageParams, ObjectsGetParams,
    StorageParamsAlt, ObjectsInsertParams,
    ObjectsCopyParams, ObjectsDeleteParams,
    BucketsListParams, BucketsService,
    Bucket,
};
use url::Url;

lazy_static! {
    static ref GCSINSTANCE_TEST_MUTEX: Mutex<()> = Mutex::new(());
}

fn https_client() -> TlsClient {
    let conn = hyper_rustls::HttpsConnector::with_native_roots();
    hyper::Client::builder().build(conn)
}

#[derive(Clone)]
pub struct GcsInstance {
    buckets: Arc<BucketsService>,
    objects: Arc<ObjectsService>,
}

impl Debug for GcsInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GcsInstance")
    }
}

impl GcsInstance {
    pub async fn new(
        gcs_token_path: &Path,
        gcs_secret_file: &Path,
        session_name: &str,
    ) -> Result<Self, Error> {
        debug!("{:?}", gcs_secret_file);
        let https = https_client();
        let sec = yup_oauth2::read_application_secret(gcs_secret_file).await?;

        let token_file = gcs_token_path.join(format!("{}.json", session_name));

        let parent = gcs_token_path;

        if !parent.exists() {
            create_dir_all(parent).await?;
        }

        debug!("{:?}", token_file);
        let auth = InstalledFlowAuthenticator::builder(
            sec,
            common::yup_oauth2::InstalledFlowReturnMethod::HTTPRedirect,
        )
        .persist_tokens_to_disk(token_file)
        .hyper_client(https.clone())
        .build()
        .await?;
        let auth = Arc::new(auth);

        let buckets = Arc::new(BucketsService::new(https.clone(), auth.clone()));
        let objects = Arc::new(ObjectsService::new(https, auth));

        Ok(Self{buckets, objects})
    }

    pub fn get_instance_lock() -> MutexGuard<'static, ()> {
        GCSINSTANCE_TEST_MUTEX.lock()
    }

    pub async fn get_list_of_keys(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>, Error> {
        let mut params = ObjectsListParams {
            bucket: bucket.into(),
            prefix: prefix.map(Into::into),
            storage_params: Some(StorageParams {
                fields: Some("*".into()),
                ..StorageParams::default()
            }),
            ..ObjectsListParams::default()
        };
        let mut npt = None;
        let mut output = Vec::new();
        loop {
            params.page_token = npt.take();
            let result = self.objects.list(&params).await?;
            if let Some(items) = result.items.as_ref() {
                output.extend_from_slice(items);
            } else {
                break;
            }
            if result.next_page_token.is_some() {
                npt = result.next_page_token.clone();
            } else {
                break;
            }
        }
        Ok(output)
    }

    pub async fn process_list_of_keys<T>(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&Object) -> Result<(), Error> + Send + Sync,
    {
        let mut params = ObjectsListParams {
            bucket: bucket.into(),
            prefix: prefix.map(Into::into),
            storage_params: Some(StorageParams {
                fields: Some("*".into()),
                ..StorageParams::default()
            }),
            ..ObjectsListParams::default()
        };
        let mut npt = None;
        loop {
            params.page_token = npt.take();
            let result = self.objects.list(&params).await?;
            if let Some(items) = result.items.as_ref() {
                for item in items {
                    callback(item)?;
                }
            } else {
                break;
            }
            if result.next_page_token.is_some() {
                npt = result.next_page_token.clone();
            } else {
                break;
            }
        }
        Ok(())
    }

    pub async fn download(
        &self,
        bucket_name: &str,
        key_name: &str,
        fname: &str,
    ) -> Result<(), Error> {
        let gparams = StorageParams {
            alt: Some(StorageParamsAlt::Media),
            ..StorageParams::default()
        };
        let params = ObjectsGetParams {
            storage_params: Some(gparams),
            bucket: bucket_name.into(),
            object: key_name.into(),
            ..ObjectsGetParams::default()
        };
        let mut f = fs::File::create(fname).await?;
        let mut download = self.objects.get(&params).await?;
        if let DownloadResult::Downloaded = download.do_it(Some(&mut f)).await? {
            Ok(())
        } else {
            Err(format_err!("Failed to download file"))
        }
    }

    pub async fn upload(
        &self,
        fname: &str,
        bucket_name: &str,
        key_name: &str,
    ) -> Result<(), Error> {
        let params = ObjectsInsertParams {
            bucket: bucket_name.into(),
            name: Some(key_name.into()),
            ..ObjectsInsertParams::default()
        };
        let obj = Object::default();
        let f = fs::File::open(fname).await?;
        self.objects.insert_resumable_upload(&params, &obj).await?
            .set_max_chunksize(1024 * 1024 * 5)?
            .upload_file(f).await?;
        Ok(())
    }

    pub async fn copy_key(
        &self,
        source: &Url,
        bucket_to: &str,
        key_to: &str,
    ) -> Result<Option<String>, Error> {
        let source_bucket = source.host_str().ok_or_else(|| format_err!("Bad source"))?;
        let source_key = &source.path()[1..];
        let params = ObjectsCopyParams {
            source_bucket: source_bucket.into(),
            source_object: source_key.into(),
            destination_bucket: bucket_to.into(),
            destination_object: key_to.into(),
            ..ObjectsCopyParams::default()
        };
        let obj = Object::default();
        let result = self.objects.copy(&params, &obj).await?;
        Ok(result.md5_hash)
    }

    pub async fn delete_key(&self, bucket_name: &str, key_name: &str) -> Result<(), Error> {
        let params = ObjectsDeleteParams {
            bucket: bucket_name.into(),
            object: key_name.into(),
            ..ObjectsDeleteParams::default()
        };
        self.objects.delete(&params).await.map_err(Into::into)
    }

    pub async fn get_list_of_buckets(&self, project: &str) -> Result<Vec<Bucket>, Error> {
        let mut params = BucketsListParams {
            project: project.into(),
            ..BucketsListParams::default()
        };
        let mut npt = None;
        let mut output = Vec::new();
        loop {
            params.page_token = npt.take();
            let result = self.buckets.list(&params).await?;
            if let Some(items) = result.items.as_ref() {
                output.extend_from_slice(items);
            } else {
                break;
            }
            if result.next_page_token.is_some() {
                npt = result.next_page_token.clone();
            } else {
                break;
            }
        }
        Ok(output)       
    }
}