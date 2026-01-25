# Plan Explain Formatting Improvements

This document describes the improved plan explain formatting functionality added to Chryso.

## Overview

The plan explain formatting has been enhanced with:

1. **Tree Structure Visualization**: Clear hierarchical display using Unicode box-drawing characters
2. **Better Information Organization**: Structured display of operator properties
3. **Configurable Output**: Customizable formatting options
4. **Expression Truncation**: Smart handling of long expressions

## Features

### Tree Structure
- Uses `├──` and `└──` for branches
- Uses `│` for continuing lines
- Clear parent-child relationships

### Enhanced Information Display
- **Logical Plans**: Shows operator types, predicates, expressions, and type information
- **Physical Plans**: Shows operator types, algorithms, costs, and implementation details
- **Expression Formatting**: Smart truncation for long expressions
- **List Formatting**: Compact display for long expression lists

### Configuration Options

The `ExplainConfig` struct provides the following options:

```rust
pub struct ExplainConfig {
    pub show_types: bool,        // Show type information
    pub show_costs: bool,        // Show cost information
    pub show_cardinality: bool,  // Show cardinality estimates
    pub compact: bool,           // Use compact formatting
    pub max_expr_length: usize,  // Maximum output length (after truncation, includes "..." when applied)
}
```

## Usage Examples

### Basic Usage

```rust
use chryso::planner::{ExplainConfig, ExplainFormatter};

let config = ExplainConfig::default();
let formatter = ExplainFormatter::new(config);

// Format logical plan
let logical_output = formatter.format_logical_plan(&logical_plan, type_inferencer);

// Format physical plan  
let physical_output = formatter.format_physical_plan(&physical_plan, cost_model);
```

### Example Output

```
=== Logical Plan ===
LogicalFilter: predicate=id = 1, type=Bool
└── LogicalScan: table=users

=== Physical Plan ===
Filter: predicate=id = 1, cost=1.00
└── TableScan: table=users, cost=1.00
```

### Complex Query Example

```
=== Logical Plan ===
LogicalLimit: limit=Some(10)
└── LogicalSort: order_by=[count(*) desc]
    └── LogicalProject: expressions=[u.name, count(*)], types=[Unknown, Int]
        └── LogicalAggregate: group_by=[[u.name]], aggregates=[[u.name, count(*)]]
            └── LogicalFilter: predicate=u.age > 18, type=Bool
                └── LogicalJoin: type=Inner, on=u.id = o.user_id
                    ├── LogicalScan: table=users
                    └── LogicalScan: table=orders
```

## Implementation Details

### New Module
- Added `crates/planner/src/explain.rs` with the formatting logic

### Key Components
- `ExplainConfig`: Configuration for formatting options
- `ExplainFormatter`: Main formatter implementation
- `format_simple_logical_plan()`: Convenience function for basic formatting
- `format_simple_physical_plan()`: Convenience function for basic formatting

### Integration
- Updated CLI to use new formatting by default
- Maintains backward compatibility with existing `explain()` methods
- All tests pass with the new implementation

## Benefits

1. **Better Readability**: Tree structure makes plan hierarchy clear
2. **More Information**: Enhanced display of operator properties
3. **Flexibility**: Configurable output for different use cases
4. **Maintainability**: Clean separation of formatting logic
5. **Extensibility**: Easy to add new formatting options

## Future Enhancements

Potential areas for future improvement:
- Color coding for different operator types
- HTML/XML output formats
- Integration with visualization tools
- More sophisticated cost formatting
- Cardinality estimation display
