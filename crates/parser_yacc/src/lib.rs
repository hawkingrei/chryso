use corundum_core::{CorundumError, CorundumResult};
use corundum_parser::{ParserConfig, SimpleParser, SqlParser};
use lrlex::lrlex_mod;
use lrpar::lrpar_mod;

lrlex_mod!("grammar/sql.l");
lrpar_mod!("grammar/sql.y");

pub struct YaccParser {
    config: ParserConfig,
}

impl YaccParser {
    pub fn new(config: ParserConfig) -> Self {
        Self { config }
    }
}

impl SqlParser for YaccParser {
    fn parse(&self, sql: &str) -> CorundumResult<corundum_core::ast::Statement> {
        if validate_with_yacc(sql).is_ok() {
            let parser = SimpleParser::new(self.config.clone());
            return parser.parse(sql);
        }
        let fallback = SimpleParser::new(self.config.clone());
        fallback.parse(sql)
    }
}

fn validate_with_yacc(sql: &str) -> CorundumResult<()> {
    let lexerdef = sql_l::lexerdef();
    let lexer = lexerdef.lexer(sql);
    let (result, errors) = sql_y::parse(&lexer);
    if !errors.is_empty() {
        let mut rendered = String::new();
        for error in errors {
            rendered.push_str(&format!("{error:?}\n"));
        }
        return Err(CorundumError::new(rendered.trim_end()));
    }
    if result.is_some() {
        Ok(())
    } else {
        Err(CorundumError::new("yacc parse failed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corundum_parser::Dialect;

    #[test]
    fn yacc_parser_falls_back() {
        let parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        let stmt = parser.parse("select 1").expect("parse");
        match stmt {
            corundum_core::ast::Statement::Select(_) => {}
            _ => panic!("expected select"),
        }
    }
}
