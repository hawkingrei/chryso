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
            return Err(ChrysoError::new("ddl handler is not configured"));
        };
        if !handler.supports(ctx, env) {
            return Err(ChrysoError::new("ddl handler does not support statement"));
        }
        let result = handler.handle(ctx, env)?;
        return Ok(PlanOutcome::DdlHandled { result });
    }

    let logical = PlanBuilder::build_from_envelope(env)?;
    authorizer.authorize_plan(ctx, env, &logical)?;
    Ok(PlanOutcome::Planned { logical })
}
