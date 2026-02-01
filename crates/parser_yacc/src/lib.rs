use chryso_core::{ChrysoError, ChrysoResult};
use chryso_parser::{ParserConfig, SimpleParser, SqlParser};

lrlex::lrlex_mod!("sql.l");
lrpar::lrpar_mod!("sql.y");

pub struct YaccParser {
    config: ParserConfig,
}

impl YaccParser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }
}

impl SqlParser for YaccParser {
    fn parse(&self, sql: &str) -> ChrysoResult<chryso_core::ast::Statement> {
        // When strict validation is enabled, surface lrpar errors instead of silently falling back.
        let strict = std::env::var("CHRYSO_YACC_STRICT")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
            .unwrap_or(false);
        let lexerdef = sql_l::lexerdef();
        let lexer = lexerdef.lexer(sql);
        let (result, errors) = sql_y::parse(&lexer);
        if strict {
            if !errors.is_empty() {
                let mut rendered = Vec::new();
                for error in errors {
                    rendered.push(error.pp(&lexer, &sql_y::token_epp).to_string());
                }
                return Err(ChrysoError::new(rendered.join("\n")));
            }
            if result.is_none() {
                return Err(ChrysoError::new(
                    "yacc parser failed to produce a statement",
                ));
            }
        }
        let parser = SimpleParser::new(self.config.clone());
        parser.parse(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chryso_parser::Dialect;

    #[test]
    fn yacc_parser_falls_back() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select 1").expect("parse");
        match stmt {
            chryso_core::ast::Statement::Select(_) => {}
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn yacc_parser_rejects_invalid_sql() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let err = parser.parse("select from").unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
