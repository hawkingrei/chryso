#[derive(Debug, Clone)]
pub enum Statement {
    With(WithStatement),
    Select(SelectStatement),
    SetOp {
        left: Box<Statement>,
        op: SetOperator,
        right: Box<Statement>,
    },
    Explain(Box<Statement>),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    Truncate(TruncateStatement),
    Analyze(AnalyzeStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementCategory {
    Query,
    Dml,
    Ddl,
    Utility,
}

impl Statement {
    pub fn category(&self) -> StatementCategory {
        match self {
            Statement::With(with) => with.statement.category(),
            Statement::Select(_) | Statement::SetOp { .. } => StatementCategory::Query,
            Statement::Explain(_) | Statement::Analyze(_) => StatementCategory::Utility,
            Statement::CreateTable(_) | Statement::DropTable(_) | Statement::Truncate(_) => {
                StatementCategory::Ddl
            }
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                StatementCategory::Dml
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTableStatement {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub struct DropTableStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct TruncateStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct AnalyzeStatement {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<Statement>),
    DefaultValues,
}

#[derive(Debug, Clone)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub returning: Vec<SelectItem>,
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
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub distinct: bool,
    pub distinct_on: Vec<Expr>,
    pub projection: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub qualify: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct WithStatement {
    pub ctes: Vec<Cte>,
    pub recursive: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<Statement>,
}

#[derive(Debug, Clone, Copy)]
pub enum SetOperator {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub factor: TableFactor,
    pub alias: Option<String>,
    pub column_aliases: Vec<String>,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone)]
pub enum TableFactor {
    Table { name: String },
    Derived { query: Box<Statement> },
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
    Right,
    Full,
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
    pub nulls_first: Option<bool>,
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
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
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
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Wildcard,
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UnaryOperator {
    Not,
    Neg,
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WindowFrameKind {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Box<Expr>),
    CurrentRow,
    Following(Box<Expr>),
    UnboundedFollowing,
}

#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
}

impl Expr {
    pub fn to_sql(&self) -> String {
        match self {
            Expr::Identifier(name) => name.clone(),
            Expr::Literal(Literal::String(value)) => {
                format!("'{}'", escape_sql_string(value))
            }
            Expr::Literal(Literal::Number(value)) => value.to_string(),
            Expr::Literal(Literal::Bool(value)) => {
                if *value {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
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
            Expr::IsNull { expr, negated } => {
                if *negated {
                    format!("{} is not null", expr.to_sql())
                } else {
                    format!("{} is null", expr.to_sql())
                }
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                if *negated {
                    format!("{} is not distinct from {}", left.to_sql(), right.to_sql())
                } else {
                    format!("{} is distinct from {}", left.to_sql(), right.to_sql())
                }
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
                            let mut rendered = format!("{} {dir}", item.expr.to_sql());
                            if let Some(nulls_first) = item.nulls_first {
                                if nulls_first {
                                    rendered.push_str(" nulls first");
                                } else {
                                    rendered.push_str(" nulls last");
                                }
                            }
                            rendered
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    clauses.push(format!("order by {order}"));
                }
                if let Some(frame) = &spec.frame {
                    clauses.push(frame_to_sql(frame));
                }
                format!("{} over ({})", function.to_sql(), clauses.join(" "))
            }
            Expr::Exists(select) => format!("exists ({})", select_to_sql(select)),
            Expr::InSubquery { expr, subquery } => {
                format!("{} in ({})", expr.to_sql(), select_to_sql(subquery))
            }
            Expr::Subquery(select) => format!("({})", select_to_sql(select)),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let mut output = String::from("case");
                if let Some(expr) = operand {
                    output.push(' ');
                    output.push_str(&expr.to_sql());
                }
                for (when_expr, then_expr) in when_then {
                    output.push_str(" when ");
                    output.push_str(&when_expr.to_sql());
                    output.push_str(" then ");
                    output.push_str(&then_expr.to_sql());
                }
                if let Some(expr) = else_expr {
                    output.push_str(" else ");
                    output.push_str(&expr.to_sql());
                }
                output.push_str(" end");
                output
            }
            Expr::Wildcard => "*".to_string(),
        }
    }

    pub fn structural_eq(&self, other: &Expr) -> bool {
        const FLOAT_EPSILON: f64 = 1e-9;
        match (self, other) {
            (Expr::Identifier(left), Expr::Identifier(right)) => left == right,
            (Expr::Literal(left), Expr::Literal(right)) => match (left, right) {
                (Literal::String(left), Literal::String(right)) => left == right,
                (Literal::Number(left), Literal::Number(right)) => {
                    if left.is_nan() || right.is_nan() {
                        false
                    } else if left.is_infinite() || right.is_infinite() {
                        left == right
                    } else {
                        (left - right).abs() <= FLOAT_EPSILON
                    }
                }
                (Literal::Bool(left), Literal::Bool(right)) => left == right,
                _ => false,
            },
            (
                Expr::UnaryOp {
                    op: left_op,
                    expr: left,
                },
                Expr::UnaryOp {
                    op: right_op,
                    expr: right,
                },
            ) => left_op == right_op && left.structural_eq(right),
            (
                Expr::BinaryOp {
                    left: left_lhs,
                    op: left_op,
                    right: left_rhs,
                },
                Expr::BinaryOp {
                    left: right_lhs,
                    op: right_op,
                    right: right_rhs,
                },
            ) => {
                left_op == right_op
                    && left_lhs.structural_eq(right_lhs)
                    && left_rhs.structural_eq(right_rhs)
            }
            (
                Expr::IsNull {
                    expr: left,
                    negated: left_negated,
                },
                Expr::IsNull {
                    expr: right,
                    negated: right_negated,
                },
            ) => left_negated == right_negated && left.structural_eq(right),
            (
                Expr::IsDistinctFrom {
                    left: left_lhs,
                    right: left_rhs,
                    negated: left_negated,
                },
                Expr::IsDistinctFrom {
                    left: right_lhs,
                    right: right_rhs,
                    negated: right_negated,
                },
            ) => {
                left_negated == right_negated
                    && left_lhs.structural_eq(right_lhs)
                    && left_rhs.structural_eq(right_rhs)
            }
            (
                Expr::FunctionCall {
                    name: left_name,
                    args: left_args,
                },
                Expr::FunctionCall {
                    name: right_name,
                    args: right_args,
                },
            ) => {
                left_name == right_name
                    && left_args.len() == right_args.len()
                    && left_args
                        .iter()
                        .zip(right_args.iter())
                        .all(|(left, right)| left.structural_eq(right))
            }
            (
                Expr::WindowFunction {
                    function: left_func,
                    spec: left_spec,
                },
                Expr::WindowFunction {
                    function: right_func,
                    spec: right_spec,
                },
            ) => {
                left_func.structural_eq(right_func)
                    && left_spec.partition_by.len() == right_spec.partition_by.len()
                    && left_spec
                        .partition_by
                        .iter()
                        .zip(right_spec.partition_by.iter())
                        .all(|(left, right)| left.structural_eq(right))
                    && left_spec.order_by.len() == right_spec.order_by.len()
                    && left_spec
                        .order_by
                        .iter()
                        .zip(right_spec.order_by.iter())
                        .all(|(left, right)| {
                            left.asc == right.asc
                                && left.nulls_first == right.nulls_first
                                && left.expr.structural_eq(&right.expr)
                        })
                    && window_frame_eq(&left_spec.frame, &right_spec.frame)
            }
            (Expr::Subquery(left), Expr::Subquery(right)) => {
                select_to_sql(left) == select_to_sql(right)
            }
            (Expr::Exists(left), Expr::Exists(right)) => {
                select_to_sql(left) == select_to_sql(right)
            }
            (
                Expr::InSubquery {
                    expr: left_expr,
                    subquery: left_subquery,
                },
                Expr::InSubquery {
                    expr: right_expr,
                    subquery: right_subquery,
                },
            ) => {
                left_expr.structural_eq(right_expr)
                    && select_to_sql(left_subquery) == select_to_sql(right_subquery)
            }
            (
                Expr::Case {
                    operand: left_operand,
                    when_then: left_when_then,
                    else_expr: left_else,
                },
                Expr::Case {
                    operand: right_operand,
                    when_then: right_when_then,
                    else_expr: right_else,
                },
            ) => {
                left_operand
                    .as_ref()
                    .zip(right_operand.as_ref())
                    .map(|(left, right)| left.structural_eq(right))
                    .unwrap_or(left_operand.is_none() && right_operand.is_none())
                    && left_when_then.len() == right_when_then.len()
                    && left_when_then
                        .iter()
                        .zip(right_when_then.iter())
                        .all(|(left, right)| {
                            left.0.structural_eq(&right.0) && left.1.structural_eq(&right.1)
                        })
                    && left_else
                        .as_ref()
                        .zip(right_else.as_ref())
                        .map(|(left, right)| left.structural_eq(right))
                        .unwrap_or(left_else.is_none() && right_else.is_none())
            }
            (Expr::Wildcard, Expr::Wildcard) => true,
            _ => false,
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
            Expr::IsNull { expr, negated } => Expr::IsNull {
                expr: Box::new(expr.normalize()),
                negated: *negated,
            },
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => Expr::IsDistinctFrom {
                left: Box::new(left.normalize()),
                right: Box::new(right.normalize()),
                negated: *negated,
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(expr.normalize()),
            },
            Expr::FunctionCall { name, args } => Expr::FunctionCall {
                name: name.clone(),
                args: args.iter().map(|arg| arg.normalize()).collect(),
            },
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.as_ref().map(|expr| Box::new(expr.normalize())),
                when_then: when_then
                    .iter()
                    .map(|(when_expr, then_expr)| (when_expr.normalize(), then_expr.normalize()))
                    .collect(),
                else_expr: else_expr.as_ref().map(|expr| Box::new(expr.normalize())),
            },
            Expr::WindowFunction { function, spec } => Expr::WindowFunction {
                function: Box::new(function.normalize()),
                spec: WindowSpec {
                    partition_by: spec
                        .partition_by
                        .iter()
                        .map(|expr| expr.normalize())
                        .collect(),
                    order_by: spec
                        .order_by
                        .iter()
                        .map(|order| OrderByExpr {
                            expr: order.expr.normalize(),
                            asc: order.asc,
                            nulls_first: order.nulls_first,
                        })
                        .collect(),
                    frame: spec.frame.as_ref().map(|frame| WindowFrame {
                        kind: frame.kind,
                        start: normalize_frame_bound(&frame.start),
                        end: frame.end.as_ref().map(normalize_frame_bound),
                    }),
                },
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

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn rewrite_strong_expr(expr: Expr) -> Expr {
    match expr {
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => match *expr {
            Expr::Literal(Literal::Bool(value)) => Expr::Literal(Literal::Bool(!value)),
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => *expr,
            Expr::IsNull { expr, negated } => Expr::IsNull {
                expr,
                negated: !negated,
            },
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => Expr::IsDistinctFrom {
                left,
                right,
                negated: !negated,
            },
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
            Expr::Literal(Literal::Number(value)) => Expr::Literal(Literal::Number(-value)),
            other => Expr::UnaryOp {
                op: UnaryOperator::Neg,
                expr: Box::new(other),
            },
        },
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) && left.structural_eq(&right)
            {
                return *left;
            }
            let same_expr = left.structural_eq(&right);
            if same_expr {
                return match op {
                    BinaryOperator::Eq | BinaryOperator::LtEq | BinaryOperator::GtEq => {
                        Expr::Literal(Literal::Bool(true))
                    }
                    BinaryOperator::NotEq | BinaryOperator::Lt | BinaryOperator::Gt => {
                        Expr::Literal(Literal::Bool(false))
                    }
                    _ => Expr::BinaryOp { left, op, right },
                };
            }
            match (*left, op, *right) {
                (
                    Expr::Literal(Literal::Bool(a)),
                    BinaryOperator::And,
                    Expr::Literal(Literal::Bool(b)),
                ) => Expr::Literal(Literal::Bool(a && b)),
                (
                    Expr::Literal(Literal::Bool(a)),
                    BinaryOperator::Or,
                    Expr::Literal(Literal::Bool(b)),
                ) => Expr::Literal(Literal::Bool(a || b)),
                (Expr::Literal(Literal::Bool(a)), BinaryOperator::And, other) => {
                    if a {
                        other
                    } else {
                        Expr::Literal(Literal::Bool(false))
                    }
                }
                (other, BinaryOperator::And, Expr::Literal(Literal::Bool(b))) => {
                    if b {
                        other
                    } else {
                        Expr::Literal(Literal::Bool(false))
                    }
                }
                (Expr::Literal(Literal::Bool(a)), BinaryOperator::Or, other) => {
                    if a {
                        Expr::Literal(Literal::Bool(true))
                    } else {
                        other
                    }
                }
                (other, BinaryOperator::Or, Expr::Literal(Literal::Bool(b))) => {
                    if b {
                        Expr::Literal(Literal::Bool(true))
                    } else {
                        other
                    }
                }
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Eq,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a == b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::NotEq,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a != b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Lt,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a < b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::LtEq,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a <= b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Gt,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a > b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::GtEq,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Bool(a >= b)),
                (
                    Expr::Literal(Literal::String(a)),
                    BinaryOperator::Eq,
                    Expr::Literal(Literal::String(b)),
                ) => Expr::Literal(Literal::Bool(a == b)),
                (
                    Expr::Literal(Literal::String(a)),
                    BinaryOperator::NotEq,
                    Expr::Literal(Literal::String(b)),
                ) => Expr::Literal(Literal::Bool(a != b)),
                (
                    Expr::Literal(Literal::Bool(a)),
                    BinaryOperator::Eq,
                    Expr::Literal(Literal::Bool(b)),
                ) => Expr::Literal(Literal::Bool(a == b)),
                (
                    Expr::Literal(Literal::Bool(a)),
                    BinaryOperator::NotEq,
                    Expr::Literal(Literal::Bool(b)),
                ) => Expr::Literal(Literal::Bool(a != b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Add,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Number(a + b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Sub,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Number(a - b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Mul,
                    Expr::Literal(Literal::Number(b)),
                ) => Expr::Literal(Literal::Number(a * b)),
                (
                    Expr::Literal(Literal::Number(a)),
                    BinaryOperator::Div,
                    Expr::Literal(Literal::Number(b)),
                ) => {
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
            }
        }
        other => other,
    }
}

pub fn normalize_statement(statement: &Statement) -> Statement {
    match statement {
        Statement::With(stmt) => Statement::With(WithStatement {
            ctes: stmt
                .ctes
                .iter()
                .map(|cte| Cte {
                    name: cte.name.clone(),
                    columns: cte.columns.clone(),
                    query: Box::new(normalize_statement(&cte.query)),
                })
                .collect(),
            recursive: stmt.recursive,
            statement: Box::new(normalize_statement(&stmt.statement)),
        }),
        Statement::Select(select) => Statement::Select(normalize_select(select)),
        Statement::SetOp { left, op, right } => Statement::SetOp {
            left: Box::new(normalize_statement(left)),
            op: *op,
            right: Box::new(normalize_statement(right)),
        },
        Statement::Explain(inner) => Statement::Explain(Box::new(normalize_statement(inner))),
        Statement::CreateTable(stmt) => Statement::CreateTable(stmt.clone()),
        Statement::DropTable(stmt) => Statement::DropTable(stmt.clone()),
        Statement::Truncate(stmt) => Statement::Truncate(stmt.clone()),
        Statement::Analyze(stmt) => Statement::Analyze(stmt.clone()),
        Statement::Insert(stmt) => Statement::Insert(InsertStatement {
            table: stmt.table.clone(),
            columns: stmt.columns.clone(),
            source: match &stmt.source {
                InsertSource::Values(values) => InsertSource::Values(
                    values
                        .iter()
                        .map(|row| row.iter().map(|expr| expr.normalize()).collect())
                        .collect(),
                ),
                InsertSource::Query(statement) => {
                    InsertSource::Query(Box::new(normalize_statement(statement)))
                }
                InsertSource::DefaultValues => InsertSource::DefaultValues,
            },
            returning: stmt
                .returning
                .iter()
                .map(|item| SelectItem {
                    expr: item.expr.normalize(),
                    alias: item.alias.clone(),
                })
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
            returning: stmt
                .returning
                .iter()
                .map(|item| SelectItem {
                    expr: item.expr.normalize(),
                    alias: item.alias.clone(),
                })
                .collect(),
        }),
        Statement::Delete(stmt) => Statement::Delete(DeleteStatement {
            table: stmt.table.clone(),
            selection: stmt.selection.as_ref().map(|expr| expr.normalize()),
            returning: stmt
                .returning
                .iter()
                .map(|item| SelectItem {
                    expr: item.expr.normalize(),
                    alias: item.alias.clone(),
                })
                .collect(),
        }),
    }
}

fn normalize_select(select: &SelectStatement) -> SelectStatement {
    SelectStatement {
        distinct: select.distinct,
        distinct_on: select
            .distinct_on
            .iter()
            .map(|expr| expr.normalize())
            .collect(),
        projection: select
            .projection
            .iter()
            .map(|item| SelectItem {
                expr: item.expr.normalize(),
                alias: item.alias.clone(),
            })
            .collect(),
        from: select.from.as_ref().map(normalize_table_ref),
        selection: select.selection.as_ref().map(|expr| expr.normalize()),
        group_by: select
            .group_by
            .iter()
            .map(|expr| expr.normalize())
            .collect(),
        having: select.having.as_ref().map(|expr| expr.normalize()),
        qualify: select.qualify.as_ref().map(|expr| expr.normalize()),
        order_by: select
            .order_by
            .iter()
            .map(|order| OrderByExpr {
                expr: order.expr.normalize(),
                asc: order.asc,
                nulls_first: order.nulls_first,
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
        factor: match &table.factor {
            TableFactor::Table { name } => TableFactor::Table { name: name.clone() },
            TableFactor::Derived { query } => TableFactor::Derived {
                query: Box::new(normalize_statement(query)),
            },
        },
        alias: table.alias.clone(),
        column_aliases: table.column_aliases.clone(),
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
        if !select.distinct_on.is_empty() {
            let distinct_on = select
                .distinct_on
                .iter()
                .map(|expr| expr.to_sql())
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str("on (");
            output.push_str(&distinct_on);
            output.push_str(") ");
        }
    }
    let projection = select
        .projection
        .iter()
        .map(|item| item.expr.to_sql())
        .collect::<Vec<_>>()
        .join(", ");
    output.push_str(&projection);
    if let Some(from) = &select.from {
        output.push_str(" from ");
        output.push_str(&table_ref_to_sql(from));
    }
    if let Some(selection) = &select.selection {
        output.push_str(" where ");
        output.push_str(&selection.to_sql());
    }
    if !select.group_by.is_empty() {
        let group_by = select
            .group_by
            .iter()
            .map(|expr| expr.to_sql())
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(" group by ");
        output.push_str(&group_by);
    }
    if let Some(having) = &select.having {
        output.push_str(" having ");
        output.push_str(&having.to_sql());
    }
    if let Some(qualify) = &select.qualify {
        output.push_str(" qualify ");
        output.push_str(&qualify.to_sql());
    }
    if !select.order_by.is_empty() {
        let order_by = select
            .order_by
            .iter()
            .map(|item| {
                let mut rendered = item.expr.to_sql();
                rendered.push(' ');
                rendered.push_str(if item.asc { "asc" } else { "desc" });
                if let Some(nulls_first) = item.nulls_first {
                    rendered.push_str(" nulls ");
                    rendered.push_str(if nulls_first { "first" } else { "last" });
                }
                rendered
            })
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(" order by ");
        output.push_str(&order_by);
    }
    if let Some(limit) = select.limit {
        output.push_str(" limit ");
        output.push_str(&limit.to_string());
    }
    if let Some(offset) = select.offset {
        output.push_str(" offset ");
        output.push_str(&offset.to_string());
    }
    output
}

fn frame_to_sql(frame: &WindowFrame) -> String {
    let kind = match frame.kind {
        WindowFrameKind::Rows => "rows",
        WindowFrameKind::Range => "range",
        WindowFrameKind::Groups => "groups",
    };
    let start = frame_bound_to_sql(&frame.start);
    if let Some(end) = &frame.end {
        format!("{kind} between {start} and {}", frame_bound_to_sql(end))
    } else {
        format!("{kind} {start}")
    }
}

fn frame_bound_to_sql(bound: &WindowFrameBound) -> String {
    match bound {
        WindowFrameBound::UnboundedPreceding => "unbounded preceding".to_string(),
        WindowFrameBound::Preceding(expr) => format!("{} preceding", expr.to_sql()),
        WindowFrameBound::CurrentRow => "current row".to_string(),
        WindowFrameBound::Following(expr) => format!("{} following", expr.to_sql()),
        WindowFrameBound::UnboundedFollowing => "unbounded following".to_string(),
    }
}

fn normalize_frame_bound(bound: &WindowFrameBound) -> WindowFrameBound {
    match bound {
        WindowFrameBound::UnboundedPreceding => WindowFrameBound::UnboundedPreceding,
        WindowFrameBound::Preceding(expr) => {
            WindowFrameBound::Preceding(Box::new(expr.normalize()))
        }
        WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(expr) => {
            WindowFrameBound::Following(Box::new(expr.normalize()))
        }
        WindowFrameBound::UnboundedFollowing => WindowFrameBound::UnboundedFollowing,
    }
}

fn window_frame_eq(left: &Option<WindowFrame>, right: &Option<WindowFrame>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.kind == right.kind
                && frame_bound_eq(&left.start, &right.start)
                && match (&left.end, &right.end) {
                    (None, None) => true,
                    (Some(a), Some(b)) => frame_bound_eq(a, b),
                    _ => false,
                }
        }
        _ => false,
    }
}

fn frame_bound_eq(left: &WindowFrameBound, right: &WindowFrameBound) -> bool {
    match (left, right) {
        (WindowFrameBound::UnboundedPreceding, WindowFrameBound::UnboundedPreceding) => true,
        (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow) => true,
        (WindowFrameBound::UnboundedFollowing, WindowFrameBound::UnboundedFollowing) => true,
        (WindowFrameBound::Preceding(a), WindowFrameBound::Preceding(b)) => a.structural_eq(b),
        (WindowFrameBound::Following(a), WindowFrameBound::Following(b)) => a.structural_eq(b),
        _ => false,
    }
}

fn table_ref_to_sql(table: &TableRef) -> String {
    let mut output = match &table.factor {
        TableFactor::Table { name } => name.clone(),
        TableFactor::Derived { query } => format!("({})", statement_to_sql(query)),
    };
    if let Some(alias) = &table.alias {
        output.push_str(" as ");
        output.push_str(alias);
        if !table.column_aliases.is_empty() {
            output.push_str(" (");
            output.push_str(&table.column_aliases.join(", "));
            output.push(')');
        }
    }
    for join in &table.joins {
        let join_type = match join.join_type {
            JoinType::Inner => "join",
            JoinType::Left => "left join",
            JoinType::Right => "right join",
            JoinType::Full => "full join",
        };
        output.push(' ');
        output.push_str(join_type);
        output.push(' ');
        output.push_str(&table_ref_to_sql(&join.right));
        output.push_str(" on ");
        output.push_str(&join.on.to_sql());
    }
    output
}

pub fn statement_to_sql(statement: &Statement) -> String {
    match statement {
        Statement::Select(select) => select_to_sql(select),
        Statement::SetOp { left, op, right } => {
            let left_sql = statement_to_sql(left);
            let right_sql = statement_to_sql(right);
            let op_str = match op {
                SetOperator::Union => "union",
                SetOperator::UnionAll => "union all",
                SetOperator::Intersect => "intersect",
                SetOperator::IntersectAll => "intersect all",
                SetOperator::Except => "except",
                SetOperator::ExceptAll => "except all",
            };
            format!("{left_sql} {op_str} {right_sql}")
        }
        Statement::With(with_stmt) => {
            let ctes = with_stmt
                .ctes
                .iter()
                .map(|cte| {
                    let mut name = cte.name.clone();
                    if !cte.columns.is_empty() {
                        let cols = cte.columns.join(", ");
                        name.push_str(" (");
                        name.push_str(&cols);
                        name.push(')');
                    }
                    format!("{name} as ({})", statement_to_sql(&cte.query))
                })
                .collect::<Vec<_>>()
                .join(", ");
            let keyword = if with_stmt.recursive {
                "with recursive"
            } else {
                "with"
            };
            format!(
                "{keyword} {ctes} {}",
                statement_to_sql(&with_stmt.statement)
            )
        }
        Statement::Explain(inner) => format!("explain {}", statement_to_sql(inner)),
        Statement::CreateTable(stmt) => {
            if stmt.if_not_exists {
                if stmt.columns.is_empty() {
                    format!("create table if not exists {}", stmt.name)
                } else {
                    let columns = stmt
                        .columns
                        .iter()
                        .map(|col| format!("{} {}", col.name, col.data_type))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("create table if not exists {} ({})", stmt.name, columns)
                }
            } else {
                if stmt.columns.is_empty() {
                    format!("create table {}", stmt.name)
                } else {
                    let columns = stmt
                        .columns
                        .iter()
                        .map(|col| format!("{} {}", col.name, col.data_type))
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("create table {} ({})", stmt.name, columns)
                }
            }
        }
        Statement::DropTable(stmt) => {
            if stmt.if_exists {
                format!("drop table if exists {}", stmt.name)
            } else {
                format!("drop table {}", stmt.name)
            }
        }
        Statement::Truncate(stmt) => format!("truncate table {}", stmt.table),
        Statement::Analyze(stmt) => format!("analyze {}", stmt.table),
        Statement::Insert(stmt) => {
            let mut output = format!("insert into {}", stmt.table);
            if !stmt.columns.is_empty() {
                output.push_str(" (");
                output.push_str(&stmt.columns.join(", "));
                output.push(')');
            }
            match &stmt.source {
                InsertSource::DefaultValues => {
                    output.push_str(" default values");
                }
                InsertSource::Values(values) => {
                    let rows = values
                        .iter()
                        .map(|row| {
                            let values = row
                                .iter()
                                .map(|expr| expr.to_sql())
                                .collect::<Vec<_>>()
                                .join(", ");
                            format!("({values})")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    output.push_str(" values ");
                    output.push_str(&rows);
                }
                InsertSource::Query(statement) => {
                    output.push(' ');
                    output.push_str(&statement_to_sql(statement));
                }
            }
            if !stmt.returning.is_empty() {
                let returning = stmt
                    .returning
                    .iter()
                    .map(|item| item.expr.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(" returning ");
                output.push_str(&returning);
            }
            output
        }
        Statement::Update(stmt) => {
            let mut output = format!("update {} set ", stmt.table);
            let assignments = stmt
                .assignments
                .iter()
                .map(|assign| format!("{} = {}", assign.column, assign.value.to_sql()))
                .collect::<Vec<_>>()
                .join(", ");
            output.push_str(&assignments);
            if let Some(selection) = &stmt.selection {
                output.push_str(" where ");
                output.push_str(&selection.to_sql());
            }
            if !stmt.returning.is_empty() {
                let returning = stmt
                    .returning
                    .iter()
                    .map(|item| item.expr.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(" returning ");
                output.push_str(&returning);
            }
            output
        }
        Statement::Delete(stmt) => {
            let mut output = format!("delete from {}", stmt.table);
            if let Some(selection) = &stmt.selection {
                output.push_str(" where ");
                output.push_str(&selection.to_sql());
            }
            if !stmt.returning.is_empty() {
                let returning = stmt
                    .returning
                    .iter()
                    .map(|item| item.expr.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(" returning ");
                output.push_str(&returning);
            }
            output
        }
    }
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

    #[test]
    fn normalize_duplicate_and_or() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Or,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Identifier(name) if name == "a"));
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::And,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Identifier(name) if name == "a"));
    }

    #[test]
    fn normalize_self_comparison() {
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Literal(Literal::Bool(true))));
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::NotEq,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Literal(Literal::Bool(false))));
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Lt,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Literal(Literal::Bool(false))));
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::LtEq,
            right: Box::new(Expr::Identifier("a".to_string())),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Literal(Literal::Bool(true))));
    }

    #[test]
    fn normalize_not_literal() {
        let expr = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::Literal(Literal::Bool(true))),
        };
        let normalized = expr.normalize();
        assert!(matches!(normalized, Expr::Literal(Literal::Bool(false))));
    }
}
