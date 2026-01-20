use corundum_core::ast::{BinaryOperator, Expr, Literal, UnaryOperator};
use crate::types::DataType;

pub trait TypeInferencer {
    fn infer_expr(&self, expr: &Expr) -> DataType;
}

pub struct SimpleTypeInferencer;

impl TypeInferencer for SimpleTypeInferencer {
    fn infer_expr(&self, expr: &Expr) -> DataType {
        match expr {
            Expr::Literal(Literal::Number(_)) => DataType::Float,
            Expr::Literal(Literal::String(_)) => DataType::String,
            Expr::Identifier(_) => DataType::Unknown,
            Expr::UnaryOp { op, .. } => match op {
                UnaryOperator::Not => DataType::Bool,
                UnaryOperator::Neg => DataType::Float,
            },
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq
                | BinaryOperator::And
                | BinaryOperator::Or => DataType::Bool,
                BinaryOperator::Add
                | BinaryOperator::Sub
                | BinaryOperator::Mul
                | BinaryOperator::Div => DataType::Float,
            },
            Expr::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "count" => DataType::Int,
                "sum" | "avg" | "min" | "max" => DataType::Float,
                _ => DataType::Unknown,
            },
            Expr::WindowFunction { function, .. } => self.infer_expr(function),
            Expr::Exists(_) | Expr::InSubquery { .. } => DataType::Bool,
            Expr::Subquery(_) => DataType::Unknown,
            Expr::Wildcard => DataType::Unknown,
        }
    }
}

pub fn infer_with_registry(
    expr: &Expr,
    registry: &crate::functions::FunctionRegistry,
) -> DataType {
    match expr {
        Expr::FunctionCall { name, .. } => registry
            .return_type(name)
            .unwrap_or(DataType::Unknown),
        Expr::WindowFunction { function, .. } => infer_with_registry(function, registry),
        Expr::Exists(_) | Expr::InSubquery { .. } => DataType::Bool,
        Expr::Subquery(_) => DataType::Unknown,
        _ => SimpleTypeInferencer.infer_expr(expr),
    }
}

pub fn expr_types(exprs: &[Expr], inferencer: &dyn TypeInferencer) -> Vec<DataType> {
    exprs.iter().map(|expr| inferencer.infer_expr(expr)).collect()
}

#[cfg(test)]
mod tests {
    use super::{SimpleTypeInferencer, TypeInferencer};
    use corundum_core::ast::{BinaryOperator, Expr, Literal};
    use crate::types::DataType;

    #[test]
    fn infer_numeric_binary() {
        let infer = SimpleTypeInferencer;
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Literal(Literal::Number(1.0))),
            op: BinaryOperator::Add,
            right: Box::new(Expr::Literal(Literal::Number(2.0))),
        };
        assert_eq!(infer.infer_expr(&expr), DataType::Float);
    }
}
