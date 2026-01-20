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
        match self {
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
            Expr::Exists(select) => Expr::Exists(Box::new((**select).clone())),
            Expr::InSubquery { expr, subquery } => Expr::InSubquery {
                expr: Box::new(expr.normalize()),
                subquery: Box::new((**subquery).clone()),
            },
            Expr::Subquery(select) => Expr::Subquery(Box::new((**select).clone())),
            other => other.clone(),
        }
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
    use super::{BinaryOperator, Expr};

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
}
