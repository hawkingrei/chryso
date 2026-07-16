# Optimizer Rules Guide

## Overview
Logical rules transform plans into equivalent alternatives. They live in
`crates/optimizer/src/rules.rs` and are registered in the `RuleSet`. Physical implementation rules
live in `crates/optimizer/src/physical_rules.rs` and are registered in `PhysicalRuleSet`.

## Adding a Rule
1. Implement `Rule` with `name()` and `apply()`.
2. Register the rule in `RuleSet::default()`.
3. Add a unit test in the rules module.

## Example Skeleton
```rust
pub struct MyRule;

impl Rule for MyRule {
    fn name(&self) -> &str {
        "my_rule"
    }

    fn apply(&self, plan: &LogicalPlan, _ctx: &mut RuleContext) -> Vec<LogicalPlan> {
        // return zero or more rewrites
        Vec::new()
    }
}
```

Logical rewrites are inserted into the source memo group. Do not return a plan unless it is
logically equivalent to the input matched by the rule. The exploration loop revisits newly added
expressions until it reaches a fixpoint or a configured search budget.

## Adding a Physical Rule
1. Implement `PhysicalRule` with `name()` and `apply()`.
2. Override `input_requirements()` when the operator preserves, supplies, or destroys properties.
3. Register the rule in `PhysicalRuleSet::default()`.
4. Add tests for both unconstrained and relevant constrained optimization goals.

`input_requirements()` returns alternative property requirements for each logical child. A
property-preserving unary operator should normally consider both propagation to its child and an
unconstrained child followed by an enforcer. An ordering operator supplies ordering but must still
propagate distribution requirements. The optimizer validates delivered properties and adds `Sort`
or `Exchange` only when needed.

## Debugging
Enable rule tracing via `OptimizerConfig { debug_rules: true, .. }` and inspect
`CascadesOptimizer::optimize_with_trace` or `CascadesOptimizer::optimize_with_memo_trace`.

The trace exposes conflict diagnostics recorded during rule application:
- `OptimizerTrace.conflict_pairs`: ordered `(lhs, rhs)` pairs for detected literal conflicts.
- `OptimizerTrace.conflicting_literals`: legacy tuple list mirroring `conflict_pairs`.

Conflict pairs are recorded deterministically for easier diffing across runs.
Memo traces also expose property-keyed goals, candidate costs, the selected alternative, and the
rule or enforcer responsible for each candidate.
