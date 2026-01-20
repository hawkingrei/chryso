use std::fmt;

#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug)]
pub struct CorundumError {
    message: String,
    span: Option<Span>,
    code: Option<ErrorCode>,
}

impl CorundumError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            span: None,
            code: None,
        }
    }

    pub fn with_span(message: impl Into<String>, span: Span) -> Self {
        Self {
            message: message.into(),
            span: Some(span),
            code: None,
        }
    }

    pub fn with_code(mut self, code: ErrorCode) -> Self {
        self.code = Some(code);
        self
    }
}

impl fmt::Display for CorundumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            write!(f, "[{:?}] ", code)?;
        }
        if let Some(span) = self.span {
            write!(f, "{} at {}..{}", self.message, span.start, span.end)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CorundumError, ErrorCode};

    #[test]
    fn error_code_in_display() {
        let err = CorundumError::new("boom").with_code(ErrorCode::OptimizerError);
        let message = err.to_string();
        assert!(message.contains("OptimizerError"));
    }
}

impl std::error::Error for CorundumError {}

pub type CorundumResult<T> = Result<T, CorundumError>;
#[derive(Debug, Clone, Copy)]
pub enum ErrorCode {
    ParserError,
    PlannerError,
    OptimizerError,
    AdapterError,
    MetadataError,
}
