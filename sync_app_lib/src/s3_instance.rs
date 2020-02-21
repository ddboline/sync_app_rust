use anyhow::{format_err, Error};
use futures::stream::{StreamExt, TryStreamExt};
use rusoto_core::Region;
use rusoto_s3::{
    Bucket, CopyObjectRequest, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest,
    GetObjectRequest, ListObjectsV2Request, Object, PutObjectRequest, S3Client, S3,
};
use s4::S4;
use std::fmt;
use std::path::Path;
use sts_profile_auth::get_client_sts;
use url::Url;

#[derive(Clone)]
pub struct S3Instance {
    s3_client: S3Client,
    max_keys: Option<usize>,
}

impl fmt::Debug for S3Instance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "S3Instance")
    }
}

impl Default for S3Instance {
    fn default() -> Self {
        Self {
            s3_client: get_client_sts!(S3Client, Region::UsEast1).expect("Failed to obtain client"),
            max_keys: None,
        }
    }
}

impl S3Instance {
    pub fn new(aws_region_name: &str) -> Self {
        let region: Region = aws_region_name.parse().ok().unwrap_or(Region::UsEast1);
        Self {
            s3_client: get_client_sts!(S3Client, region).expect("Failed to obtain client"),
            max_keys: None,
        }
    }

    pub fn max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    pub async fn get_list_of_buckets(&self) -> Result<Vec<Bucket>, Error> {
        self.s3_client
            .list_buckets()
            .await
            .map(|l| l.buckets.unwrap_or_default())
            .map_err(Into::into)
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> Result<String, Error> {
        let req = CreateBucketRequest {
            bucket: bucket_name.to_string(),
            ..CreateBucketRequest::default()
        };
        self.s3_client
            .create_bucket(req)
            .await?
            .location
            .ok_or_else(|| format_err!("Failed to create bucket"))
    }

    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<(), Error> {
        let req = DeleteBucketRequest {
            bucket: bucket_name.to_string(),
        };
        self.s3_client.delete_bucket(req).await.map_err(Into::into)
    }

    pub async fn delete_key(&self, bucket_name: &str, key_name: &str) -> Result<(), Error> {
        let req = DeleteObjectRequest {
            bucket: bucket_name.to_string(),
            key: key_name.to_string(),
            ..DeleteObjectRequest::default()
        };
        self.s3_client
            .delete_object(req)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn copy_key(
        &self,
        source: &Url,
        bucket_to: &str,
        key_to: &str,
    ) -> Result<Option<String>, Error> {
        let copy_source = source.to_string();
        let req = CopyObjectRequest {
            copy_source,
            bucket: bucket_to.to_string(),
            key: key_to.to_string(),
            ..CopyObjectRequest::default()
        };
        self.s3_client
            .copy_object(req)
            .await
            .map_err(Into::into)
            .map(|x| x.copy_object_result.and_then(|s| s.e_tag))
    }

    pub async fn upload(
        &self,
        fname: &str,
        bucket_name: &str,
        key_name: &str,
    ) -> Result<(), Error> {
        if !Path::new(fname).exists() {
            return Err(format_err!("File doesn't exist {}", fname));
        }
        let req = PutObjectRequest {
            bucket: bucket_name.to_string(),
            key: key_name.to_string(),
            ..PutObjectRequest::default()
        };
        self.s3_client
            .upload_from_file(fname, req)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    pub async fn download(
        &self,
        bucket_name: &str,
        key_name: &str,
        fname: &str,
    ) -> Result<String, Error> {
        let req = GetObjectRequest {
            bucket: bucket_name.to_string(),
            key: key_name.to_string(),
            ..GetObjectRequest::default()
        };
        self.s3_client
            .download_to_file(req, fname)
            .await
            .map(|x| {
                x.e_tag
                    .as_ref()
                    .map_or("", |y| y.trim_matches('"'))
                    .to_string()
            })
            .map_err(Into::into)
    }

    pub async fn get_list_of_keys(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<Object>, Error> {
        let stream = match prefix {
            Some(p) => self.s3_client.iter_objects_with_prefix(bucket, p),
            None => self.s3_client.iter_objects(bucket),
        }
        .into_stream();
        let results: Result<Vec<_>, _> = match self.max_keys {
            Some(nkeys) => stream.take(nkeys).try_collect().await,
            None => stream.try_collect().await,
        };
        results.map_err(Into::into)
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
        let mut stream = match prefix {
            Some(p) => self.s3_client.stream_objects_with_prefix(bucket, p),
            None => self.s3_client.stream_objects(bucket),
        };
        let mut nkeys = 0;
        while let Some(item) = stream.try_next().await? {
            callback(&item)?;
            nkeys += 1;
            if let Some(keys) = self.max_keys {
                if nkeys > keys {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use std::io::{stdout, Write};

    use crate::s3_instance::S3Instance;

    #[tokio::test]
    #[ignore]
    async fn test_list_buckets() -> Result<(), Error> {
        let s3_instance = S3Instance::new("us-east-1").max_keys(100);
        let blist = s3_instance.get_list_of_buckets().await?;
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());
        let klist = s3_instance.get_list_of_keys(&bucket, None).await?;
        writeln!(stdout(), "{} {}", bucket, klist.len())?;
        assert!(klist.len() > 0);
        Ok(())
    }
}
