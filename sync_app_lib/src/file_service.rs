use failure::{err_msg, Error};
use std::fmt;
use std::str::FromStr;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum FileService {
    Local,
    GDrive,
    OneDrive,
    S3,
    SSH,
}

impl Default for FileService {
    fn default() -> FileService {
        FileService::Local
    }
}

impl FromStr for FileService {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(FileService::Local),
            "gdrive" => Ok(FileService::GDrive),
            "onedrive" => Ok(FileService::OneDrive),
            "s3" => Ok(FileService::S3),
            "ssh" => Ok(FileService::SSH),
            _ => Err(err_msg("Failed to parse FileService")),
        }
    }
}

impl fmt::Display for FileService {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileService::Local => write!(f, "local"),
            FileService::GDrive => write!(f, "gdrive"),
            FileService::OneDrive => write!(f, "onedrive"),
            FileService::S3 => write!(f, "s3"),
            FileService::SSH => write!(f, "ssh"),
        }
    }
}