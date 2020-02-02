use derive_more::{AsRef, Deref, DerefMut, Display, From, Into};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;
use url::Url;

#[derive(Debug, Clone, From, Into, PartialEq, Eq, Deref, DerefMut, Display, AsRef)]
pub struct UrlWrapper(pub Url);

impl FromStr for UrlWrapper {
    type Err = url::ParseError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Url::from_str(input).map(Into::into)
    }
}

impl Serialize for UrlWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.as_str())
    }
}

impl<'de> Deserialize<'de> for UrlWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).and_then(|s| s.parse().map_err(serde::de::Error::custom))
    }
}
