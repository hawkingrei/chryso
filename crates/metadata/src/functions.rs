use crate::types::DataType;

#[derive(Debug, Clone)]
pub struct FunctionSignature {
    pub name: String,
    pub return_type: DataType,
}

#[derive(Debug, Default)]
pub struct FunctionRegistry {
    functions: std::collections::HashMap<String, FunctionSignature>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            functions: std::collections::HashMap::new(),
        }
    }

    pub fn register(&mut self, name: impl Into<String>, return_type: DataType) {
        let name = name.into();
        let key = name.to_ascii_lowercase();
        self.functions
            .insert(key, FunctionSignature { name, return_type });
    }

    pub fn is_known(&self, name: &str) -> bool {
        self.functions.contains_key(&name.to_ascii_lowercase())
    }

    pub fn return_type(&self, name: &str) -> Option<DataType> {
        self.functions
            .get(&name.to_ascii_lowercase())
            .map(|sig| sig.return_type.clone())
    }

    pub fn with_builtins() -> Self {
        let mut registry = FunctionRegistry::new();
        registry.register("count", DataType::Int);
        registry.register("sum", DataType::Float);
        registry.register("avg", DataType::Float);
        registry.register("min", DataType::Float);
        registry.register("max", DataType::Float);
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::FunctionRegistry;
    use crate::types::DataType;

    #[test]
    fn registry_resolves_builtin() {
        let registry = FunctionRegistry::with_builtins();
        assert_eq!(registry.return_type("sum"), Some(DataType::Float));
    }
}
