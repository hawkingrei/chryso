#[derive(Debug, Clone)]
pub enum LogLevel {
    Trace,
    Info,
    Warn,
    Error,
}

pub trait Logger {
    fn log(&self, level: LogLevel, message: &str);
}

#[derive(Debug, Default)]
pub struct NoopLogger;

impl Logger for NoopLogger {
    fn log(&self, _level: LogLevel, _message: &str) {}
}
