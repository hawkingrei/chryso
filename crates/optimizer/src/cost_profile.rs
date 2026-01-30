use crate::cost::CostModelConfig;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostProfile {
    pub config: CostModelConfig,
    pub notes: Option<String>,
}

impl CostProfile {
    pub fn load_from_path(path: impl AsRef<Path>) -> chryso_core::error::ChrysoResult<Self> {
        let content = fs::read_to_string(path.as_ref()).map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("read cost profile failed: {err}"))
        })?;
        let profile: CostProfile = if path
            .as_ref()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("toml"))
            .unwrap_or(false)
        {
            toml::from_str(&content).map_err(|err| {
                chryso_core::error::ChrysoError::new(format!(
                    "parse toml cost profile failed: {err}"
                ))
            })?
        } else {
            serde_json::from_str(&content).map_err(|err| {
                chryso_core::error::ChrysoError::new(format!(
                    "parse json cost profile failed: {err}"
                ))
            })?
        };
        profile.config.validate()?;
        Ok(profile)
    }
}
