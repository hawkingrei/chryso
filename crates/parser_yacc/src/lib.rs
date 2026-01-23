use chryso_core::ChrysoResult;
use chryso_parser::{ParserConfig, SimpleParser, SqlParser};

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
}
