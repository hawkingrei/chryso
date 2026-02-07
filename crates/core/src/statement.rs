use crate::ast::{Statement, StatementCategory};

#[derive(Debug, Clone, Copy, Default)]
pub struct NoExtension;

#[derive(Debug, Clone)]
pub struct StatementContext {
    original_sql: String,
}

impl StatementContext {
    pub fn new(original_sql: impl Into<String>) -> Self {
        Self {
            original_sql: original_sql.into(),
        }
    }

    pub fn original_sql(&self) -> &str {
        &self.original_sql
    }
}

#[derive(Debug, Clone)]
pub struct StatementEnvelope<E = NoExtension> {
    pub statement: Statement,
    pub category: StatementCategory,
    pub context: StatementContext,
    pub extension: Option<E>,
}

impl StatementEnvelope<NoExtension> {
    pub fn new(statement: Statement, context: StatementContext) -> Self {
        let category = statement.category();
        Self {
            statement,
            category,
            context,
            extension: None,
        }
    }
}

impl<E> StatementEnvelope<E> {
    pub fn new_with_extension(
        statement: Statement,
        context: StatementContext,
        extension: E,
    ) -> Self {
        let category = statement.category();
        Self {
            statement,
            category,
            context,
            extension: Some(extension),
        }
    }
}
