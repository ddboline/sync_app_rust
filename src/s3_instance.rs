use failure::{err_msg, Error};
use rusoto_core::Region;
use rusoto_s3::{
    Bucket, CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, GetObjectRequest,
    ListObjectsV2Request, Object, PutObjectRequest, S3Client, S3,
};
use s4::S4;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct S3Instance {
    s3_client: Arc<S3Client>,
}

impl fmt::Debug for S3Instance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Arc<S3Instance>")
    }
}

impl S3Instance {
    pub fn new(region_name: Option<&str>) -> Self {
        let region: Region = match region_name {
            Some(r) => r.parse().ok(),
            None => None,
        }
        .unwrap_or(Region::UsEast1);
        Self {
            s3_client: Arc::new(S3Client::new(region)),
        }
    }

    pub fn get_list_of_buckets(&self) -> Result<Vec<Bucket>, Error> {
        self.s3_client
            .list_buckets()
            .sync()
            .map(|l| l.buckets.unwrap_or_default())
            .map_err(err_msg)
    }

    pub fn create_bucket(&self, bucket_name: &str) -> Result<String, Error> {
        self.s3_client
            .create_bucket(CreateBucketRequest {
                bucket: bucket_name.to_string(),
                ..Default::default()
            })
            .sync()?
            .location
            .ok_or_else(|| err_msg("Failed to create bucket"))
    }

    pub fn delete_bucket(&self, bucket_name: &str) -> Result<(), Error> {
        self.s3_client
            .delete_bucket(DeleteBucketRequest {
                bucket: bucket_name.to_string(),
            })
            .sync()
            .map_err(err_msg)
    }

    pub fn delete_key(&self, bucket_name: &str, key_name: &str) -> Result<(), Error> {
        self.s3_client
            .delete_object(DeleteObjectRequest {
                bucket: bucket_name.to_string(),
                key: key_name.to_string(),
                ..Default::default()
            })
            .sync()
            .map(|_| ())
            .map_err(err_msg)
    }

    pub fn upload(&self, fname: &str, bucket_name: &str, key_name: &str) -> Result<(), Error> {
        if Path::new(&fname).exists() {
            return Err(err_msg("File doesn't exist"));
        }
        self.s3_client.upload_from_file(
            &fname,
            PutObjectRequest {
                bucket: bucket_name.to_string(),
                key: key_name.to_string(),
                ..Default::default()
            },
        )?;
        Ok(())
    }

    pub fn download(
        &self,
        bucket_name: &str,
        key_name: &str,
        fname: &str,
    ) -> Result<String, Error> {
        self.s3_client
            .download_to_file(
                GetObjectRequest {
                    bucket: bucket_name.to_string(),
                    key: key_name.to_string(),
                    ..Default::default()
                },
                &fname,
            )
            .map(|x| x.e_tag.unwrap_or_else(|| "".to_string()))
            .map_err(err_msg)
    }

    pub fn get_list_of_keys<T>(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        callback: T,
    ) -> Result<(), Error>
    where
        T: Fn(&Object) -> () + Send + Sync,
    {
        let mut continuation_token = None;

        loop {
            let current_list = self
                .s3_client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.to_string(),
                    continuation_token,
                    prefix: prefix.map(ToString::to_string),
                    ..Default::default()
                })
                .sync()?;

            continuation_token = current_list.next_continuation_token.clone();

            match current_list.key_count {
                Some(0) => (),
                Some(_) => {
                    for item in current_list.contents.unwrap_or_else(Vec::new) {
                        callback(&item);
                    }
                }
                None => (),
            };

            match &continuation_token {
                Some(_) => (),
                None => break,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::s3_instance::S3Instance;

    #[test]
    fn test_list_buckets() {
        let s3_instance = S3Instance::new(None);
        let blist = s3_instance.get_list_of_buckets().unwrap();
        let bucket = blist
            .get(0)
            .and_then(|b| b.name.clone())
            .unwrap_or_else(|| "".to_string());
        let mut klist = Vec::new();
        s3_instance
            .get_list_of_keys(&bucket, None, |i| klist.push(i.clone()))
            .unwrap();
        println!("{} {}", bucket, klist.len());
        assert!(klist.len() > 0);
    }
}
