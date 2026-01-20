# Optimizer Rules Guide

## Overview
Rules transform logical plans into equivalent alternatives. They live in `crates/optimizer/src/rules.rs`
and are registered in the `RuleSet`.

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

    fn apply(&self, plan: &LogicalPlan) -> Vec<LogicalPlan> {
        // return zero or more rewrites
        Vec::new()
    }
}
```

## Debugging
Enable rule tracing via `OptimizerConfig { debug_rules: true, .. }` and inspect
`CascadesOptimizer::optimize_with_trace`.
