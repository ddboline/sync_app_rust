use derive_more::{AsRef, Deref, DerefMut, From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    convert::AsRef,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, From, Into, PartialEq, Eq, Deref, DerefMut, AsRef)]
pub struct PathBufWrapper(pub PathBuf);

impl From<String> for PathBufWrapper {
    fn from(item: String) -> Self {
        Self(item.into())
    }
}

impl AsRef<Path> for PathBufWrapper {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl Serialize for PathBufWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.to_string_lossy())
    }
}

impl<'de> Deserialize<'de> for PathBufWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Into::into)
    }
}
