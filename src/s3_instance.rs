use failure::{err_msg, Error};
use rusoto_core::Region;
use rusoto_s3::{
    Bucket, CreateBucketRequest, DeleteBucketRequest, GetObjectRequest, ListObjectsV2Request,
    Object, PutObjectRequest, S3Client, S3,
};
use s4::S4;
use std::path::Path;

pub struct S3Instance {
    s3_client: S3Client,
}

impl S3Instance {
    pub fn new(region_name: Option<&str>) -> Self {
        let region: Region = match region_name {
            Some(r) => r.parse().ok(),
            None => None,
        }
        .unwrap_or(Region::UsEast1);
        Self {
            s3_client: S3Client::new(region),
        }
    }

    pub fn get_list_of_buckets(&self) -> Result<Vec<Bucket>, Error> {
        self.s3_client
            .list_buckets()
            .sync()
            .map(|l| l.buckets.unwrap_or(Vec::new()))
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

    pub fn get_list_of_keys(&self, bucket: &str) -> Result<Vec<Object>, Error> {
        let mut continuation_token = None;

        let mut list_of_keys = Vec::new();

        loop {
            let current_list = self
                .s3_client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.to_string(),
                    continuation_token,
                    ..Default::default()
                })
                .sync()?;

            continuation_token = current_list.next_continuation_token.clone();

            match current_list.key_count {
                Some(0) => (),
                Some(_) => {
                    for item in current_list.contents.unwrap_or_else(Vec::new) {
                        list_of_keys.push(item.clone())
                    }
                }
                None => (),
            };

            match &continuation_token {
                Some(_) => (),
                None => break,
            };
            if list_of_keys.len() > 100 {
                break;
            }
        }

        Ok(list_of_keys)
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
        let klist = s3_instance.get_list_of_keys(&bucket).unwrap();
        println!("{} {}", bucket, klist.len());
        assert!(klist.len() > 0);
    }
}