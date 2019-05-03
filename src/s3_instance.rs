use failure::{err_msg, Error};
use rusoto_core::Region;
use rusoto_s3::{Bucket, ListObjectsV2Request, Object, S3Client, S3};

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
        println!("{}", klist.len());
        assert_eq!(klist.len(), 1000);
    }
}
