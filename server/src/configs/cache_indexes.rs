use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::str::FromStr;
use strum::Display;

#[serde_as]
#[derive(Debug, Serialize, Display, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CacheIndexesConfig {
    All,
    #[default]
    #[serde(rename = "open_segment")]
    OpenSegment,
    None,
}

impl<'de> Deserialize<'de> for CacheIndexesConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrBool {
            String(String),
            Bool(bool),
        }

        let value = StringOrBool::deserialize(deserializer)?;

        match value {
            StringOrBool::String(s) => match s.to_lowercase().as_str() {
                "all" | "true" => Ok(CacheIndexesConfig::All),
                "open_segment" => Ok(CacheIndexesConfig::OpenSegment),
                "none" | "false" => Ok(CacheIndexesConfig::None),
                _ => Err(serde::de::Error::custom(format!(
                    "Invalid CacheIndexesConfig value: {}",
                    s
                ))),
            },
            StringOrBool::Bool(b) => {
                if b {
                    Ok(CacheIndexesConfig::All)
                } else {
                    Ok(CacheIndexesConfig::None)
                }
            }
        }
    }
}

impl FromStr for CacheIndexesConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" | "true" => Ok(CacheIndexesConfig::All),
            "open_segment" => Ok(CacheIndexesConfig::OpenSegment),
            "none" | "false" => Ok(CacheIndexesConfig::None),
            _ => Err(format!("Invalid CacheIndexesConfig: {}", s)),
        }
    }
}

impl From<bool> for CacheIndexesConfig {
    fn from(value: bool) -> Self {
        if value {
            CacheIndexesConfig::All
        } else {
            CacheIndexesConfig::None
        }
    }
}
