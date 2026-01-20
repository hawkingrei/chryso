use crate::optimizer::OptimizerConfig;

#[derive(Debug)]
pub struct EngineConfig {
    pub optimizer: OptimizerConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            optimizer: OptimizerConfig::default(),
        }
    }
}
