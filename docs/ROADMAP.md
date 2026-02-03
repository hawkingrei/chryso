# Chryso Roadmap (100 Steps)

## MVP Success Criteria
- Parse PG/MySQL-style SELECT with joins, group by, having, order by, limit.
- Build logical and physical plans with explain output.
- Run cascades-style optimization skeleton with rule hooks.
- Lower physical plans into DuckDB-compatible SQL.
- Provide test helpers for execute + explain.

## Steps
TODO: Add Bazel build support for workspace targets.
TODO: Use DuckDB `SUMMARIZE <table>` output to backfill StatsCache (histogram/top-N if available).
TODO: Add stats cache status reporting (in-memory footprint, loaded vs missing).
TODO: Add richer AST/Logical rewrite rules (e.g., `a=a`, `a!=a`, null-aware simplifications).
1. done - Confirm scope and MVP success criteria
2. done - Define module boundaries and public API surface
3. in-progress - Establish error model and diagnostic spans
4. done - Finalize AST node set for SELECT/exprs
5. done - Add tokenizer with punctuation and literals
6. done - Add keyword table per dialect
7. done - Add precedence parser (OR/AND/comparison/arithmetic)
8. done - Add unary operators and function calls
9. done - Add identifier quoting rules (pg/mysql)
10. done - Add SELECT list items with aliases
11. done - Add FROM with table aliases
12. done - Add JOIN types (inner/left) and ON parsing
13. done - Add GROUP BY/HAVING/ORDER BY/LIMIT parsing
14. done - Add OFFSET parsing
15. done - Add DISTINCT parsing
16. done - Add basic DDL/EXPLAIN placeholder statements
17. done - Add parser error recovery and nicer messages
18. done - Add parser golden tests for dialect differences
19. done - Define logical plan node set
20. done - Implement logical plan builder for new clauses
21. done - Add expression normalization utilities
22. done - Define physical plan node set
23. done - Add plan explain formatting improvements
24. done - Introduce memo data structures
25. done - Add group/group expr insertion from logical tree
26. done - Implement rule trait and rule registry
27. done - Add filter-merge rule
28. done - Add projection-prune rule
29. done - Add filter-pushdown rule
30. done - Add predicate simplification rule
31. done - Add join reordering rule skeleton
32. done - Add aggregate pushdown rule skeleton
33. done - Add physical implementation rules (scan/filter/join/agg)
34. done - Add physical property framework (ordering, distribution)
35. done - Add memo exploration loop
36. done - Add cost model trait and default cost
37. done - Add cardinality estimation interface
38. done - Add statistics cache schema
39. done - Add table/column stats structures
40. done - Add analyze statement parsing
41. done - Add analyze execution hook into metadata
42. done - Add catalog interface for tables and schemas
43. done - Add mock catalog implementation
44. done - Add logical validation step (name resolution)
45. done - Add type inference skeleton
46. done - Add type coercion rules
47. done - Add logical plan pretty printer with types
48. done - Add physical plan pretty printer with costs
49. done - Add optimizer tracing hooks
50. done - Add rule debug toggles/config
51. done - Add DuckDB adapter trait mapping
52. done - Add SQL lowering for scan/filter/projection
53. done - Add SQL lowering for join
54. done - Add SQL lowering for aggregate
55. done - Add SQL lowering for sort/limit/offset
56. done - Add DuckDB parameter binding interface
57. done - Add DuckDB result conversion utilities
58. done - Add adapter capability flags
59. done - Add adapter fallback behavior for unsupported ops
60. done - Add mock adapter enhancements for tests
61. done - Add integration test harness
62. done - Add parser unit tests for precedence and joins
63. done - Add planner unit tests for group/agg
64. done - Add optimizer unit tests for rules
65. done - Add memo unit tests for group insertion
66. done - Add cost model unit tests
67. done - Add analyze tests with mock stats
68. done - Add adapter SQL snapshot tests
69. done - Add EXPLAIN output tests
70. done - Add benchmark harness for parser
71. done - Add benchmark harness for optimizer
72. done - Add example queries in docs
73. done - Add developer guide on adding rules
74. done - Add developer guide on adding adapters
75. done - Add API stability note for FFI
76. done - Add C ABI crate skeleton
77. done - Add FFI-safe plan/expr serialization
78. done - Add Go bindings skeleton
79. done - Add Java bindings skeleton
80. done - Add config system for optimizer toggles
81. done - Add logging/tracing integration
82. done - Add error codes and structured diagnostics
83. done - Add SQL formatting utility for tests
84. done - Add plan diffing utility
85. done - Add rule conflict detection
86. done - Add join algorithm selection (hash/nested loop)
87. done - Add index scan representation
88. done - Add cost for join selection
89. done - Add distribution traits for parallel engines
90. done - Add physical property enforcement rules
91. done - Add extensible function registry
92. done - Add UDF handling in parser/planner
93. done - Add window function parsing
94. done - Add limit pushdown rule
95. done - Add top-N optimization rule
96. done - Add subquery parsing (in/from/exists)
97. done - Add correlated subquery rewrite skeleton
98. done - Add CBO join order enumeration
99. done - Add plan serialization for external engines
100. done - Add end-to-end demo with DuckDB adapter
