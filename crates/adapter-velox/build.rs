use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=CHRYSO_VELOX_FFI_DIR");
    let Ok(value) = env::var("CHRYSO_VELOX_FFI_DIR") else {
        return;
    };
    let path = PathBuf::from(value);
    println!("cargo:rustc-link-search=native={}", path.display());
    println!("cargo:rustc-link-lib=chryso_velox_ffi");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", path.display());
}
