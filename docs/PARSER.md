# Parser Roadmap

## Targets
- SQL dialects: PostgreSQL + MySQL first.
- Output: unified AST with minimal dialect-specific flags.
- Maintainability: small tokenizer + recursive-descent parser over time.

## Current State
- Tokenizer with punctuation support and a recursive-descent expression parser.
- WHERE supports OR/AND/comparisons and arithmetic in expressions.
- SELECT supports distinct, joins, group by, having, order by, limit, and offset.
- Quoted identifiers with `"` or `` ` ``.
- EXPLAIN SELECT and CREATE TABLE placeholders are parsed.
- Yacc-style grammar scaffold lives in `crates/parser_yacc/grammar` for a future LALR-based parser.

## Planned Steps
1. Tokenizer with punctuation handling (comma, parens, operators).
2. Expression parser with precedence (OR/AND, comparison, arithmetic).
3. FROM/JOIN/ORDER/GROUP/HAVING/LIMIT.
4. Identifier quoting and dialect-specific keywords.
5. Error recovery and diagnostic spans.

## Dialect Strategy
- Parser config selects dialect.
- Tokenization is shared; dialect influences keyword set and identifier rules.
- AST remains stable across dialects to isolate optimizer.

## Yacc Track
- `crates/parser_yacc` provides a `YaccParser` shim and yacc/lex files.
- Current: yacc grammar validates SQL shape; AST is still produced by `SimpleParser`.
- Plan: generate a dedicated parser per dialect grammar file, mapping to `corundum_core::ast`.
