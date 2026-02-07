use crate::{ChrysoError, ChrysoResult, LogicalPlan, PlanBuilder, StatementCategory};
use chryso_core::statement::StatementEnvelope;

#[derive(Debug, Clone)]
pub struct DdlResult {
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub enum PlanOutcome {
    Planned { logical: LogicalPlan },
    DdlHandled { result: DdlResult },
}

pub trait DdlHandler<Ctx, E> {
    fn supports(&self, ctx: &Ctx, env: &StatementEnvelope<E>) -> bool;
    fn handle(&self, ctx: &Ctx, env: &StatementEnvelope<E>) -> ChrysoResult<DdlResult>;
}

pub trait Authorizer<Ctx, E> {
    fn authorize_statement(&self, ctx: &Ctx, env: &StatementEnvelope<E>) -> ChrysoResult<()>;

    fn authorize_plan(
        &self,
        _ctx: &Ctx,
        _env: &StatementEnvelope<E>,
        _plan: &LogicalPlan,
    ) -> ChrysoResult<()> {
        Ok(())
    }
}

pub fn plan_with_hooks<Ctx, E>(
    ctx: &Ctx,
    env: &StatementEnvelope<E>,
    authorizer: &dyn Authorizer<Ctx, E>,
    ddl_handler: Option<&dyn DdlHandler<Ctx, E>>,
) -> ChrysoResult<PlanOutcome> {
    authorizer.authorize_statement(ctx, env)?;

    if env.category == StatementCategory::Ddl {
        let Some(handler) = ddl_handler else {
            return Err(ChrysoError::new(format!(
                "ddl handler is not configured (category={:?}, sql={})",
                env.category,
                env.context.original_sql()
            )));
        };
        if !handler.supports(ctx, env) {
            return Err(ChrysoError::new(format!(
                "ddl handler does not support statement (category={:?}, sql={})",
                env.category,
                env.context.original_sql()
            )));
        }
        let result = handler.handle(ctx, env)?;
        return Ok(PlanOutcome::DdlHandled { result });
    }

    let logical = PlanBuilder::build_from_envelope(env)?;
    authorizer.authorize_plan(ctx, env, &logical)?;
    Ok(PlanOutcome::Planned { logical })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chryso_core::ast::{
        ColumnDef, CreateTableStatement, Expr, SelectItem, SelectStatement, Statement, TableFactor,
        TableRef,
    };
    use chryso_core::statement::{NoExtension, StatementContext};
    use std::cell::Cell;

    struct CountingAuthorizer {
        statement_calls: Cell<u32>,
        plan_calls: Cell<u32>,
    }

    impl CountingAuthorizer {
        fn new() -> Self {
            Self {
                statement_calls: Cell::new(0),
                plan_calls: Cell::new(0),
            }
        }
    }

    impl Authorizer<(), NoExtension> for CountingAuthorizer {
        fn authorize_statement(
            &self,
            _ctx: &(),
            _env: &StatementEnvelope<NoExtension>,
        ) -> ChrysoResult<()> {
            self.statement_calls.set(self.statement_calls.get() + 1);
            Ok(())
        }

        fn authorize_plan(
            &self,
            _ctx: &(),
            _env: &StatementEnvelope<NoExtension>,
            _plan: &LogicalPlan,
        ) -> ChrysoResult<()> {
            self.plan_calls.set(self.plan_calls.get() + 1);
            Ok(())
        }
    }

    struct CountingDdlHandler {
        supports: bool,
        calls: Cell<u32>,
    }

    impl CountingDdlHandler {
        fn new(supports: bool) -> Self {
            Self {
                supports,
                calls: Cell::new(0),
            }
        }
    }

    impl DdlHandler<(), NoExtension> for CountingDdlHandler {
        fn supports(&self, _ctx: &(), _env: &StatementEnvelope<NoExtension>) -> bool {
            self.supports
        }

        fn handle(
            &self,
            _ctx: &(),
            _env: &StatementEnvelope<NoExtension>,
        ) -> ChrysoResult<DdlResult> {
            self.calls.set(self.calls.get() + 1);
            Ok(DdlResult {
                detail: Some("ok".to_string()),
            })
        }
    }

    fn ddl_env(sql: &str) -> StatementEnvelope<NoExtension> {
        let stmt = Statement::CreateTable(CreateTableStatement {
            name: "t".to_string(),
            if_not_exists: false,
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "int".to_string(),
            }],
        });
        StatementEnvelope::new(stmt, StatementContext::new(sql))
    }

    fn select_env(sql: &str) -> StatementEnvelope<NoExtension> {
        let stmt = Statement::Select(SelectStatement {
            distinct: false,
            distinct_on: Vec::new(),
            projection: vec![SelectItem {
                expr: Expr::Identifier("id".to_string()),
                alias: None,
            }],
            from: Some(TableRef {
                factor: TableFactor::Table {
                    name: "t".to_string(),
                },
                alias: None,
                column_aliases: Vec::new(),
                joins: Vec::new(),
            }),
            selection: None,
            group_by: Vec::new(),
            having: None,
            qualify: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        });
        StatementEnvelope::new(stmt, StatementContext::new(sql))
    }

    #[test]
    fn ddl_without_handler_returns_error() {
        let env = ddl_env("create table t (id int)");
        let authorizer = CountingAuthorizer::new();
        let err = plan_with_hooks(&(), &env, &authorizer, None).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("ddl handler is not configured"));
        assert!(message.contains("create table"));
        assert_eq!(authorizer.statement_calls.get(), 1);
        assert_eq!(authorizer.plan_calls.get(), 0);
    }

    #[test]
    fn ddl_handler_supports_false_returns_error() {
        let env = ddl_env("create table t (id int)");
        let authorizer = CountingAuthorizer::new();
        let handler = CountingDdlHandler::new(false);
        let err = plan_with_hooks(&(), &env, &authorizer, Some(&handler)).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("ddl handler does not support statement"));
        assert_eq!(handler.calls.get(), 0);
        assert_eq!(authorizer.statement_calls.get(), 1);
        assert_eq!(authorizer.plan_calls.get(), 0);
    }

    #[test]
    fn ddl_handler_handles_and_skips_authorize_plan() {
        let env = ddl_env("create table t (id int)");
        let authorizer = CountingAuthorizer::new();
        let handler = CountingDdlHandler::new(true);
        let outcome = plan_with_hooks(&(), &env, &authorizer, Some(&handler)).unwrap();
        assert!(matches!(outcome, PlanOutcome::DdlHandled { .. }));
        assert_eq!(handler.calls.get(), 1);
        assert_eq!(authorizer.statement_calls.get(), 1);
        assert_eq!(authorizer.plan_calls.get(), 0);
    }

    #[test]
    fn non_ddl_authorizes_plan() {
        let env = select_env("select id from t");
        let authorizer = CountingAuthorizer::new();
        let handler = CountingDdlHandler::new(true);
        let outcome = plan_with_hooks(&(), &env, &authorizer, Some(&handler)).unwrap();
        assert!(matches!(outcome, PlanOutcome::Planned { .. }));
        assert_eq!(handler.calls.get(), 0);
        assert_eq!(authorizer.statement_calls.get(), 1);
        assert_eq!(authorizer.plan_calls.get(), 1);
    }
}
