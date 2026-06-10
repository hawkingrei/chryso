# Chryso Cost Model

This document describes the current cost model used by the optimizer. The model is intentionally small and deterministic so rule and plan changes can be tested before Chryso grows a full engine-specific costing layer.

## Cost Inputs

`CostModelConfig` defines one coefficient per physical operator class:

| Field | Meaning |
| --- | --- |
| `scan` | Base cost for `TableScan` and `IndexScan`. |
| `filter` | Base cost for evaluating a filter operator. |
| `projection` | Base cost for projection and expression materialization. |
| `join` | Base cost for join operators before algorithm multipliers. |
| `sort` | Base cost for sort and top-N operators. |
| `aggregate` | Base cost for aggregate operators. |
| `limit` | Base cost for limit and offset operators. |
| `derived` | Base cost for derived table boundaries. |
| `dml` | Base cost for DML passthrough plans. |
| `join_hash_multiplier` | Multiplier applied to hash joins. Must be at least `1.0`. |
| `join_nested_multiplier` | Multiplier applied to nested loop joins. Must be at least `1.0`. |
| `max_cost` | Clamp for invalid, infinite, or very large costs. |

All fields must be finite and positive. Invalid configs are rejected when loaded directly. During optimization, invalid runtime config falls back to defaults and records an optimizer trace warning.

## Cost Modes

The optimizer picks the cost model from the available stats:

- If `StatsCache` is empty, it uses `UnitCostModelWithConfig`. This counts weighted physical operators and is stable for tests that do not need table cardinalities.
- If `StatsCache` contains table stats, it uses `StatsCostModel`. This multiplies local operator weights by estimated rows and adds child subtree costs.

The stats model currently uses table row counts and simple selectivity heuristics. Missing table stats fall back to default estimates, so cost-sensitive tests should inject synthetic stats for every table involved in the query.

## Join Costing

Join cost uses the `join` coefficient plus an algorithm multiplier:

- Hash join uses `join_hash_multiplier`.
- Nested loop join uses `join_nested_multiplier`.

The default nested loop multiplier is higher than the hash multiplier, so hash join should win when both algorithms are otherwise comparable.

Join order selection is controlled separately from join algorithm costing. By default, the optimizer enables greedy join reordering and the `join_commute` rule. Use `--no-reorder` in CLI tools to preserve input join order for rule isolation and reproducibility checks.

## Runtime Overrides

`CostModelConfig` can be changed in three ways:

1. Load a config file with `CostModelConfig::load_from_path`.
2. Use a `CostProfile`, which pairs a named profile with a `CostModelConfig`.
3. Override individual coefficients through `SystemParamRegistry`.

The supported system parameter names are:

```text
optimizer.cost.scan
optimizer.cost.filter
optimizer.cost.projection
optimizer.cost.join
optimizer.cost.sort
optimizer.cost.aggregate
optimizer.cost.limit
optimizer.cost.derived
optimizer.cost.dml
optimizer.cost.join_hash_multiplier
optimizer.cost.join_nested_multiplier
optimizer.cost.max_cost
```

Tenant-specific system params override default params when a tenant id is provided.

## Tuning Workflow

For repeatable optimizer tuning:

1. Prepare representative SQL files under a query directory.
2. Inject or load stats before comparing plans when cardinality should affect the result.
3. Run `chryso-tune --queries <dir>` to emit `query,cost,time_ms`.
4. Run `chryso-tune --queries <dir> --no-reorder` to isolate cost changes from join order changes.
5. Use `chryso-cli --dump-rule-order` to record the optimizer rule order used for the run.
6. Use `chryso-cli --dump-memo --memo-best-only <sql>` when a cost change needs candidate-level evidence.

Keep cost regression assertions focused on stable plan properties, such as join algorithm, operator order, and relative cost changes under the same stats snapshot.
