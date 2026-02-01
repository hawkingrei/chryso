use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const DEFAULT_TENANT: &str = "__default__";

#[derive(Debug, Clone, PartialEq)]
pub enum SystemParamValue {
    Float(f64),
    Int(i64),
    Bool(bool),
    String(String),
}

impl SystemParamValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            SystemParamValue::Float(value) => Some(*value),
            SystemParamValue::Int(value) => Some(*value as f64),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct SystemParamRegistry {
    tenants: RwLock<HashMap<String, HashMap<String, SystemParamValue>>>,
}

impl SystemParamRegistry {
    pub fn new() -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn set_param(&self, tenant: &str, key: impl Into<String>, value: SystemParamValue) {
        let mut guard = self.tenants.write().expect("system param lock");
        let entry = guard.entry(tenant.to_string()).or_default();
        entry.insert(key.into(), value);
    }

    pub fn set_default_param(&self, key: impl Into<String>, value: SystemParamValue) {
        self.set_param(DEFAULT_TENANT, key, value);
    }

    pub fn get_param(&self, tenant: Option<&str>, key: &str) -> Option<SystemParamValue> {
        let guard = self.tenants.read().expect("system param lock");
        if let Some(tenant) = tenant {
            if let Some(params) = guard.get(tenant) {
                if let Some(value) = params.get(key) {
                    return Some(value.clone());
                }
            }
        }
        guard
            .get(DEFAULT_TENANT)
            .and_then(|params| params.get(key))
            .cloned()
    }

    pub fn get_f64(&self, tenant: Option<&str>, key: &str) -> Option<f64> {
        self.get_param(tenant, key).and_then(|value| value.as_f64())
    }
}

#[cfg(test)]
mod tests {
    use super::{SystemParamRegistry, SystemParamValue};

    #[test]
    fn registry_uses_default_fallback() {
        let registry = SystemParamRegistry::new();
        registry.set_default_param("optimizer.cost.scan", SystemParamValue::Float(2.0));
        assert_eq!(
            registry.get_f64(Some("tenant-a"), "optimizer.cost.scan"),
            Some(2.0)
        );
    }

    #[test]
    fn registry_prefers_tenant_specific_value() {
        let registry = SystemParamRegistry::new();
        registry.set_default_param("optimizer.cost.scan", SystemParamValue::Float(2.0));
        registry.set_param(
            "tenant-a",
            "optimizer.cost.scan",
            SystemParamValue::Float(3.0),
        );
        assert_eq!(
            registry.get_f64(Some("tenant-a"), "optimizer.cost.scan"),
            Some(3.0)
        );
    }
}
