use std::path::Path;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let grammar_dir = Path::new(&manifest_dir).join("grammar");
    let lexer_path = grammar_dir.join("sql.l");
    let parser_path = grammar_dir.join("sql.y");
    let lexer_out = Path::new(&out_dir).join("sql.l.rs");
    let parser_out = Path::new(&out_dir).join("sql.y.rs");

    lrlex::CTLexerBuilder::new()
        .lexer_path(&lexer_path)
        .output_path(&lexer_out)
        .lrpar_config(move |ctp| {
            ctp.yacckind(cfgrammar::yacc::YaccKind::Original(
                cfgrammar::yacc::YaccOriginalActionKind::GenericParseTree,
            ))
            .error_on_conflicts(false)
            .grammar_path(&parser_path)
            .output_path(&parser_out)
        })
        .build()
        .unwrap();
}
