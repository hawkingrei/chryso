use chryso_adapter::QueryResult;
use chryso_core::error::{ChrysoError, ChrysoResult};

#[cfg(feature = "velox-ffi")]
mod sys {
    use std::os::raw::{c_char, c_int, c_uchar, c_ulonglong};

    #[repr(C)]
    pub struct VxSession {
        _private: [u8; 0],
    }

    unsafe extern "C" {
        pub fn vx_session_new() -> *mut VxSession;
        pub fn vx_session_free(session: *mut VxSession);
        pub fn vx_plan_execute(
            session: *mut VxSession,
            plan_json: *const c_char,
            result_out: *mut *mut c_char,
        ) -> c_int;
        pub fn vx_plan_execute_arrow(
            session: *mut VxSession,
            plan_json: *const c_char,
            data_out: *mut *mut c_uchar,
            len_out: *mut c_ulonglong,
        ) -> c_int;
        pub fn vx_last_error() -> *const c_char;
        pub fn vx_string_free(value: *mut c_char);
        pub fn vx_bytes_free(value: *mut c_uchar);
    }
}

#[cfg(feature = "velox-ffi")]
pub fn execute_plan(plan_ir: &str) -> ChrysoResult<QueryResult> {
    use std::ffi::{CStr, CString};
    use std::ptr;

    let plan = CString::new(plan_ir).map_err(|err| ChrysoError::new(err.to_string()))?;
    unsafe {
        let session = sys::vx_session_new();
        if session.is_null() {
            return Err(ChrysoError::new("velox session init failed"));
        }
        let mut out = ptr::null_mut();
        let rc = sys::vx_plan_execute(session, plan.as_ptr(), &mut out);
        let result = if rc == 0 && !out.is_null() {
            let payload = CStr::from_ptr(out).to_string_lossy().into_owned();
            sys::vx_string_free(out);
            parse_result(&payload)
        } else {
            let err = sys::vx_last_error();
            let msg = if err.is_null() {
                "velox execution failed".to_string()
            } else {
                CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            Err(ChrysoError::new(msg))
        };
        sys::vx_session_free(session);
        result
    }
}

#[cfg(feature = "velox-ffi")]
pub fn execute_plan_arrow(plan_ir: &str) -> ChrysoResult<Vec<u8>> {
    use std::ffi::{CStr, CString};
    use std::ptr;

    let plan = CString::new(plan_ir).map_err(|err| ChrysoError::new(err.to_string()))?;
    unsafe {
        let session = sys::vx_session_new();
        if session.is_null() {
            return Err(ChrysoError::new("velox session init failed"));
        }
        let mut out = ptr::null_mut();
        let mut len: u64 = 0;
        let rc = sys::vx_plan_execute_arrow(session, plan.as_ptr(), &mut out, &mut len);
        let result = if rc == 0 && !out.is_null() && len > 0 {
            let slice = std::slice::from_raw_parts(out as *const u8, len as usize);
            let buffer = slice.to_vec();
            sys::vx_bytes_free(out);
            Ok(buffer)
        } else {
            let err = sys::vx_last_error();
            let msg = if err.is_null() {
                "velox arrow execution failed".to_string()
            } else {
                CStr::from_ptr(err).to_string_lossy().into_owned()
            };
            Err(ChrysoError::new(msg))
        };
        sys::vx_session_free(session);
        result
    }
}

#[cfg(not(feature = "velox-ffi"))]
#[allow(dead_code)]
pub fn execute_plan(_plan_ir: &str) -> ChrysoResult<QueryResult> {
    Err(ChrysoError::new("velox ffi is not enabled in this build"))
}

#[cfg(not(feature = "velox-ffi"))]
#[allow(dead_code)]
pub fn execute_plan_arrow(_plan_ir: &str) -> ChrysoResult<Vec<u8>> {
    Err(ChrysoError::new("velox ffi is not enabled in this build"))
}

#[cfg(feature = "velox-ffi")]
fn parse_result(payload: &str) -> ChrysoResult<QueryResult> {
    let mut lines = payload.lines();
    let header = lines.next().unwrap_or_default();
    let columns = if header.is_empty() {
        Vec::new()
    } else {
        header.split('\t').map(|value| value.to_string()).collect()
    };
    let mut rows = Vec::new();
    for line in lines {
        rows.push(line.split('\t').map(|value| value.to_string()).collect());
    }
    Ok(QueryResult { columns, rows })
}
