#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    Explain(Box<Statement>),
    CreateTable(CreateTableStatement),
    Analyze(AnalyzeStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct AnalyzeStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub table: String,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub projection: Vec<SelectItem>,
    pub from: TableRef,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: JoinType,
    pub right: TableRef,
    pub on: Expr,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Identifier(String),
    Literal(Literal),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    WindowFunction {
        function: Box<Expr>,
        spec: WindowSpec,
    },
    Subquery(Box<SelectStatement>),
    Exists(Box<SelectStatement>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStatement>,
    },
    Wildcard,
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOperator {
    Not,
    Neg,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

impl Expr {
    pub fn to_sql(&self) -> String {
        match self {
            Expr::Identifier(name) => name.clone(),
            Expr::Literal(Literal::String(value)) => format!("'{}'", value),
            Expr::Literal(Literal::Number(value)) => value.to_string(),
            Expr::Literal(Literal::Bool(value)) => {
                if *value { "true".to_string() } else { "false".to_string() }
            }
            Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    BinaryOperator::Eq => "=",
                    BinaryOperator::NotEq => "!=",
                    BinaryOperator::Lt => "<",
                    BinaryOperator::LtEq => "<=",
                    BinaryOperator::Gt => ">",
                    BinaryOperator::GtEq => ">=",
                    BinaryOperator::And => "and",
                    BinaryOperator::Or => "or",
                    BinaryOperator::Add => "+",
                    BinaryOperator::Sub => "-",
                    BinaryOperator::Mul => "*",
                    BinaryOperator::Div => "/",
                };
                format!("{} {} {}", left.to_sql(), op_str, right.to_sql())
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => format!("not {}", expr.to_sql()),
                UnaryOperator::Neg => format!("-{}", expr.to_sql()),
            },
            Expr::FunctionCall { name, args } => {
                let args_sql = args.iter().map(|arg| arg.to_sql()).collect::<Vec<_>>();
                format!("{}({})", name, args_sql.join(", "))
            }
            Expr::WindowFunction { function, spec } => {
                let mut clauses = Vec::new();
                if !spec.partition_by.is_empty() {
                    let partition = spec
                        .partition_by
                        .iter()
                        .map(|expr| expr.to_sql())
                        .collect::<Vec<_>>()
                        .join(", ");
                    clauses.push(format!("partition by {partition}"));
                }
                if !spec.order_by.is_empty() {
                    let order = spec
                        .order_by
                        .iter()
                        .map(|item| {
                            let dir = if item.asc { "asc" } else { "desc" };
                            format!("{} {dir}", item.expr.to_sql())
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    clauses.push(format!("order by {order}"));
                }
                format!("{} over ({})", function.to_sql(), clauses.join(" "))
            }
            Expr::Exists(select) => format!("exists ({})", select_to_sql(select)),
            Expr::InSubquery { expr, subquery } => {
                format!("{} in ({})", expr.to_sql(), select_to_sql(subquery))
            }
            Expr::Subquery(select) => format!("({})", select_to_sql(select)),
            Expr::Wildcard => "*".to_string(),
        }
    }

    pub fn normalize(&self) -> Expr {
        let normalized = match self {
            Expr::BinaryOp { left, op, right } => {
                let left_norm = left.normalize();
                let right_norm = right.normalize();
                if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    if left_norm.to_sql() > right_norm.to_sql() {
                        return Expr::BinaryOp {
                            left: Box::new(right_norm),
                            op: *op,
                            right: Box::new(left_norm),
                        };
                    }
                }
                Expr::BinaryOp {
                    left: Box::new(left_norm),
                    op: *op,
                    right: Box::new(right_norm),
                }
            }
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(expr.normalize()),
            },
            Expr::FunctionCall { name, args } => Expr::FunctionCall {
                name: name.clone(),
                args: args.iter().map(|arg| arg.normalize()).collect(),
            },
            Expr::WindowFunction { function, spec } => Expr::WindowFunction {
                function: Box::new(function.normalize()),
                spec: spec.clone(),
            },
            Expr::Exists(select) => Expr::Exists(Box::new(normalize_select_inner(select))),
            Expr::InSubquery { expr, subquery } => Expr::InSubquery {
                expr: Box::new(expr.normalize()),
                subquery: Box::new(normalize_select_inner(subquery)),
            },
            Expr::Subquery(select) => Expr::Subquery(Box::new(normalize_select_inner(select))),
            other => other.clone(),
        };
        rewrite_strong_expr(normalized)
    }
}

fn rewrite_strong_expr(expr: Expr) -> Expr {
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => match *expr {
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => *expr,
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::NotEq,
                    right,
                },
                BinaryOperator::NotEq => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                },
                BinaryOperator::Lt => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::GtEq,
                    right,
                },
                BinaryOperator::LtEq => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Gt,
                    right,
                },
                BinaryOperator::Gt => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::LtEq,
                    right,
                },
                BinaryOperator::GtEq => Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Lt,
                    right,
                },
                BinaryOperator::And => Expr::BinaryOp {
                    left: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: left,
                    }),
                    op: BinaryOperator::Or,
                    right: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: right,
                    }),
                },
                BinaryOperator::Or => Expr::BinaryOp {
                    left: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: left,
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(Expr::UnaryOp {
                        op: UnaryOperator::Not,
                        expr: right,
                    }),
                },
                _ => Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::BinaryOp { left, op, right }),
                },
            },
            other => Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(other),
            },
        },
        Expr::UnaryOp {
            op: UnaryOperator::Neg,
            expr,
        } => match *expr {
            Expr::Literal(Literal::Number(value)) => {
                Expr::Literal(Literal::Number(-value))
            }
            other => Expr::UnaryOp {
                op: UnaryOperator::Neg,
                expr: Box::new(other),
            },
        },
        Expr::BinaryOp { left, op, right } => match (*left, op, *right) {
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::And, Expr::Literal(Literal::Bool(b))) => {
                Expr::Literal(Literal::Bool(a && b))
            }
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::Or, Expr::Literal(Literal::Bool(b))) => {
                Expr::Literal(Literal::Bool(a || b))
            }
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::And, other) => {
                if a { other } else { Expr::Literal(Literal::Bool(false)) }
            }
            (other, BinaryOperator::And, Expr::Literal(Literal::Bool(b))) => {
                if b { other } else { Expr::Literal(Literal::Bool(false)) }
            }
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::Or, other) => {
                if a { Expr::Literal(Literal::Bool(true)) } else { other }
            }
            (other, BinaryOperator::Or, Expr::Literal(Literal::Bool(b))) => {
                if b { Expr::Literal(Literal::Bool(true)) } else { other }
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Eq, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a == b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::NotEq, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a != b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Lt, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a < b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::LtEq, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a <= b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Gt, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a > b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::GtEq, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Bool(a >= b))
            }
            (Expr::Literal(Literal::String(a)), BinaryOperator::Eq, Expr::Literal(Literal::String(b))) => {
                Expr::Literal(Literal::Bool(a == b))
            }
            (Expr::Literal(Literal::String(a)), BinaryOperator::NotEq, Expr::Literal(Literal::String(b))) => {
                Expr::Literal(Literal::Bool(a != b))
            }
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::Eq, Expr::Literal(Literal::Bool(b))) => {
                Expr::Literal(Literal::Bool(a == b))
            }
            (Expr::Literal(Literal::Bool(a)), BinaryOperator::NotEq, Expr::Literal(Literal::Bool(b))) => {
                Expr::Literal(Literal::Bool(a != b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Add, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Number(a + b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Sub, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Number(a - b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Mul, Expr::Literal(Literal::Number(b))) => {
                Expr::Literal(Literal::Number(a * b))
            }
            (Expr::Literal(Literal::Number(a)), BinaryOperator::Div, Expr::Literal(Literal::Number(b))) => {
                if b == 0.0 {
                    Expr::BinaryOp {
                        left: Box::new(Expr::Literal(Literal::Number(a))),
                        op: BinaryOperator::Div,
                        right: Box::new(Expr::Literal(Literal::Number(b))),
                    }
                } else {
                    Expr::Literal(Literal::Number(a / b))
                }
            }
            (left, op, right) => Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
        },
        other => other,
    }
}

pub fn normalize_statement(statement: &Statement) -> Statement {
    match statement {
        Statement::Select(select) => Statement::Select(normalize_select(select)),
        Statement::Explain(inner) => Statement::Explain(Box::new(normalize_statement(inner))),
        Statement::CreateTable(stmt) => Statement::CreateTable(stmt.clone()),
        Statement::Analyze(stmt) => Statement::Analyze(stmt.clone()),
        Statement::Insert(stmt) => Statement::Insert(InsertStatement {
            table: stmt.table.clone(),
            columns: stmt.columns.clone(),
            values: stmt
                .values
                .iter()
                .map(|row| row.iter().map(|expr| expr.normalize()).collect())
                .collect(),
        }),
        Statement::Update(stmt) => Statement::Update(UpdateStatement {
            table: stmt.table.clone(),
            assignments: stmt
                .assignments
                .iter()
                .map(|assign| Assignment {
                    column: assign.column.clone(),
                    value: assign.value.normalize(),
                })
                .collect(),
            selection: stmt.selection.as_ref().map(|expr| expr.normalize()),
        }),
        Statement::Delete(stmt) => Statement::Delete(DeleteStatement {
            table: stmt.table.clone(),
            selection: stmt.selection.as_ref().map(|expr| expr.normalize()),
        }),
    }
}

fn normalize_select(select: &SelectStatement) -> SelectStatement {
    SelectStatement {
        distinct: select.distinct,
        projection: select
            .projection
            .iter()
            .map(|item| SelectItem {
                expr: item.expr.normalize(),
                alias: item.alias.clone(),
            })
            .collect(),
        from: normalize_table_ref(&select.from),
        selection: select.selection.as_ref().map(|expr| expr.normalize()),
        group_by: select.group_by.iter().map(|expr| expr.normalize()).collect(),
        having: select.having.as_ref().map(|expr| expr.normalize()),
        order_by: select
            .order_by
            .iter()
            .map(|order| OrderByExpr {
                expr: order.expr.normalize(),
                asc: order.asc,
            })
            .collect(),
        limit: select.limit,
        offset: select.offset,
    }
}

fn normalize_select_inner(select: &SelectStatement) -> SelectStatement {
    normalize_select(select)
}

fn normalize_table_ref(table: &TableRef) -> TableRef {
    TableRef {
        name: table.name.clone(),
        alias: table.alias.clone(),
        joins: table
            .joins
            .iter()
            .map(|join| Join {
                join_type: join.join_type,
                right: normalize_table_ref(&join.right),
                on: join.on.normalize(),
            })
            .collect(),
    }
}

fn select_to_sql(select: &SelectStatement) -> String {
    let mut output = String::from("select ");
    if select.distinct {
        output.push_str("distinct ");
    }
    let projection = select
        .projection
        .iter()
        .map(|item| item.expr.to_sql())
        .collect::<Vec<_>>()
        .join(", ");
    output.push_str(&projection);
    output.push_str(" from ");
    output.push_str(&select.from.name);
    if let Some(selection) = &select.selection {
        output.push_str(" where ");
        output.push_str(&selection.to_sql());
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{BinaryOperator, Expr, Literal, UnaryOperator};

    #[test]
    fn normalize_commutative_predicate() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("b".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        let Expr::BinaryOp { left, right, .. } = normalized else {
            panic!("expected binary op");
        };
        assert!(matches!(left.as_ref(), Expr::Identifier(name) if name == "a"));
        assert!(matches!(right.as_ref(), Expr::Identifier(name) if name == "b"));
    }

    #[test]
    fn normalize_not_comparison() {
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".to_string())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier("b".to_string())),
            }),
        };
        let normalized = expr.normalize();
        let Expr::BinaryOp { op, .. } = normalized else {
            panic!("expected binary op");
        };
        assert!(matches!(op, BinaryOperator::NotEq));
    }

    #[test]
    fn normalize_double_not() {
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Identifier("flag".to_string())),
            }),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Identifier(_)));
    }

    #[test]
    fn normalize_constant_fold() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Number(2.0))),
            op: BinaryOperator::Mul,
            right: Box::new(Expr::Literal(Literal::Number(4.0))),
        };
        let normalized = expr.normalize();
        match normalized {
            Expr::Literal(Literal::Number(value)) => assert_eq!(value, 8.0),
            other => panic!("expected literal, got {other:?}"),
        }
    }

    #[test]
    fn normalize_boolean_identities() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("flag".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Identifier(_)));
    }
}
