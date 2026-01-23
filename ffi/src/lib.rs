use chryso::parser::{Dialect, ParserConfig, SimpleParser, SqlParser};

#[repr(C)]
pub struct ChrysoHandle {
    _private: [u8; 0],
}

#[unsafe(no_mangle)]
pub extern "C" fn chryso_parse_sql(sql: *const std::os::raw::c_char) -> *mut ChrysoHandle {
    if sql.is_null() {
        return std::ptr::null_mut();
    }
    let c_str = unsafe { std::ffi::CStr::from_ptr(sql) };
    let Ok(sql_str) = c_str.to_str() else {
        return std::ptr::null_mut();
    };
    let parser = SimpleParser::new(ParserConfig {
        dialect: Dialect::Postgres,
    });
    let _ = parser.parse(sql_str).ok();
    std::ptr::null_mut()
}
