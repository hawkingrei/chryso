use chryso_adapter::QueryResult;
use chryso_core::error::{ChrysoError, ChrysoResult};

#[cfg(feature = "duckdb-ops-ffi")]
mod native {
    use super::{ChrysoError, ChrysoResult, QueryResult};
    use serde::Deserialize;
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_int};
    use std::ptr::NonNull;

    #[repr(C)]
    pub struct DuckDbSession {
        _private: [u8; 0],
    }

    unsafe extern "C" {
        pub fn chryso_duckdb_session_new() -> *mut DuckDbSession;
        pub fn chryso_duckdb_session_free(session: *mut DuckDbSession);
        pub fn chryso_duckdb_plan_execute(
            session: *mut DuckDbSession,
            plan_ptr: *const u8,
            plan_len: usize,
            result_out: *mut *mut c_char,
        ) -> c_int;
        pub fn chryso_duckdb_last_error() -> *mut c_char;
        pub fn chryso_duckdb_string_free(value: *mut c_char);
    }

    #[derive(Debug, Deserialize)]
    struct RawResult {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    }

    #[derive(Debug)]
    pub struct DuckDbOpsSession {
        ptr: NonNull<DuckDbSession>,
    }

    impl DuckDbOpsSession {
        pub fn new() -> ChrysoResult<Self> {
            let session = unsafe { chryso_duckdb_session_new() };
            let ptr = NonNull::new(session)
                .ok_or_else(|| ChrysoError::new("duckdb ops session init failed"))?;
            Ok(Self { ptr })
        }

        pub fn execute_plan(&self, plan: &[u8]) -> ChrysoResult<QueryResult> {
            let mut result_ptr: *mut c_char = std::ptr::null_mut();
            let status = unsafe {
                chryso_duckdb_plan_execute(
                    self.ptr.as_ptr(),
                    plan.as_ptr(),
                    plan.len(),
                    &mut result_ptr,
                )
            };
            if status != 0 {
                let message = unsafe { last_error_message() };
                return Err(ChrysoError::new(message));
            }
            let result_json = unsafe { take_c_string(result_ptr)? };
            let parsed: RawResult = serde_json::from_str(&result_json).map_err(|err| {
                ChrysoError::new(format!("duckdb ops parse result failed: {err}"))
            })?;
            Ok(QueryResult {
                columns: parsed.columns,
                rows: parsed.rows,
            })
        }
    }

    impl Drop for DuckDbOpsSession {
        fn drop(&mut self) {
            unsafe { chryso_duckdb_session_free(self.ptr.as_ptr()) };
        }
    }

    unsafe fn take_c_string(ptr: *mut c_char) -> ChrysoResult<String> {
        if ptr.is_null() {
            return Err(ChrysoError::new("duckdb ops returned null result"));
        }
        let text = CStr::from_ptr(ptr)
            .to_str()
            .map_err(|err| ChrysoError::new(format!("duckdb ops utf8 error: {err}")))?
            .to_string();
        unsafe { chryso_duckdb_string_free(ptr) };
        Ok(text)
    }

    unsafe fn last_error_message() -> String {
        let ptr = unsafe { chryso_duckdb_last_error() };
        if ptr.is_null() {
            return "duckdb ops error".to_string();
        }
        let message = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
        unsafe { chryso_duckdb_string_free(ptr) };
        message
    }

    #[allow(dead_code)]
    fn _to_c_string(value: &str) -> CString {
        CString::new(value).unwrap_or_else(|_| CString::new("").expect("empty cstring"))
    }
}

#[cfg(feature = "duckdb-ops-ffi")]
pub use native::DuckDbOpsSession;

#[cfg(not(feature = "duckdb-ops-ffi"))]
#[derive(Debug)]
pub struct DuckDbOpsSession;

#[cfg(not(feature = "duckdb-ops-ffi"))]
impl DuckDbOpsSession {
    pub fn new() -> ChrysoResult<Self> {
        Err(ChrysoError::new(
            "duckdb ops adapter requires feature \"duckdb-ops-ffi\"",
        ))
    }

    pub fn execute_plan(&self, _plan: &[u8]) -> ChrysoResult<QueryResult> {
        Err(ChrysoError::new(
            "duckdb ops adapter requires feature \"duckdb-ops-ffi\"",
        ))
    }
}
