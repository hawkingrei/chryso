#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Bool,
    String,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TypedExpr {
    pub data_type: DataType,
}
