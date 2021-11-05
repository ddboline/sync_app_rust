use anyhow::{format_err, Error};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Copy, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum FileService {
    Local,
    GCS,
    GDrive,
    OneDrive,
    S3,
    SSH,
}

impl Default for FileService {
    fn default() -> Self {
        Self::Local
    }
}

impl FromStr for FileService {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local" => Ok(Self::Local),
            "gdrive" => Ok(Self::GDrive),
            "onedrive" => Ok(Self::OneDrive),
            "s3" => Ok(Self::S3),
            "gs" => Ok(Self::GCS),
            "ssh" => Ok(Self::SSH),
            _ => Err(format_err!("Failed to parse FileService")),
        }
    }
}

impl FileService {
    pub fn to_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::GDrive => "gdrive",
            Self::OneDrive => "onedrive",
            Self::S3 => "s3",
            Self::GCS => "gs",
            Self::SSH => "ssh",
        }
    }
}

impl fmt::Display for FileService {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
