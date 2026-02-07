#[derive(Debug, Clone)]
pub struct SessionContext {
    pub database: String,
    pub user: String,
    pub default_schema: String,
}

impl SessionContext {
    pub fn new(
        database: impl Into<String>,
        user: impl Into<String>,
        default_schema: impl Into<String>,
    ) -> Self {
        Self {
            database: database.into(),
            user: user.into(),
            default_schema: default_schema.into(),
        }
    }

    pub fn single_tenant(database: impl Into<String>) -> Self {
        Self::new(database, "default", "main")
    }
}

impl Default for SessionContext {
    fn default() -> Self {
        Self::single_tenant("default")
    }
}
