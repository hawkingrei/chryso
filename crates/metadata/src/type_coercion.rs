use crate::types::DataType;

pub trait TypeCoercion {
    fn coerce(&self, left: DataType, right: DataType) -> DataType;
}

pub struct SimpleCoercion;

impl TypeCoercion for SimpleCoercion {
    fn coerce(&self, left: DataType, right: DataType) -> DataType {
        if left == right {
            return left;
        }
        match (left, right) {
            (DataType::Int, DataType::Float) | (DataType::Float, DataType::Int) => DataType::Float,
            (DataType::String, DataType::Int)
            | (DataType::String, DataType::Float)
            | (DataType::Int, DataType::String)
            | (DataType::Float, DataType::String) => DataType::String,
            _ => DataType::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SimpleCoercion, TypeCoercion};
    use crate::types::DataType;

    #[test]
    fn coerce_int_float() {
        let coercion = SimpleCoercion;
        assert_eq!(
            coercion.coerce(DataType::Int, DataType::Float),
            DataType::Float
        );
    }
}
