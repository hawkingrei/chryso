use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=CHRYSO_VELOX_FFI_DIR");
    println!("cargo:rerun-if-env-changed=CHRYSO_VELOX_ENABLE_RPATH");
    let velox_ffi_enabled = env::var_os("CARGO_FEATURE_VELOX_FFI").is_some();
    let is_release = env::var("PROFILE").map(|value| value == "release").unwrap_or(false);
    let Ok(value) = env::var("CHRYSO_VELOX_FFI_DIR") else {
        if velox_ffi_enabled {
            println!(
                "cargo:warning=velox-ffi feature is enabled, but CHRYSO_VELOX_FFI_DIR is not set"
            );
            if is_release {
                panic!("velox-ffi feature enabled but CHRYSO_VELOX_FFI_DIR is unset");
            }
        }
        return;
    };
    let path = PathBuf::from(value);
    println!("cargo:rustc-link-search=native={}", path.display());
    println!("cargo:rustc-link-lib=chryso_velox_ffi");
    if cfg!(target_family = "unix") && env::var("CHRYSO_VELOX_ENABLE_RPATH").is_ok() {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path.display());
    }
}
