use crate::cost::{CostModelConfig, load_config_from_path};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CostProfile {
    pub config: CostModelConfig,
    pub notes: Option<String>,
}

impl CostProfile {
    pub fn load_from_path(path: impl AsRef<Path>) -> chryso_core::error::ChrysoResult<Self> {
        let profile: CostProfile = load_config_from_path(path, "cost profile")?;
        profile.config.validate()?;
        Ok(profile)
    }
}

impl Default for CostProfile {
    fn default() -> Self {
        Self {
            config: CostModelConfig::default(),
            notes: None,
        }
    }
}
