use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=CHRYSO_DUCKDB_FFI_DIR");
    println!("cargo:rerun-if-env-changed=CHRYSO_DUCKDB_USE_SUBMODULE");
    let ffi_enabled = env::var_os("CARGO_FEATURE_DUCKDB_OPS_FFI").is_some();
    if !ffi_enabled {
        return;
    }
    let path = match env::var("CHRYSO_DUCKDB_FFI_DIR") {
        Ok(value) => PathBuf::from(value),
        Err(_) => {
            let (build_dir, source_dir) = default_build_paths();
            emit_rerun_if_changed(&source_dir);
            build_duckdb_ffi(&source_dir, &build_dir);
            build_dir
        }
    };
    println!("cargo:rustc-link-search=native={}", path.display());
    println!("cargo:rustc-link-lib=chryso_duckdb_ffi");
}

fn default_build_paths() -> (PathBuf, PathBuf) {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_dir = manifest_dir
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| panic!("failed to locate workspace root from CARGO_MANIFEST_DIR"));
    let source_dir = workspace_dir.join("ffi").join("duckdb");
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let build_dir = out_dir.join("duckdb-ffi");
    (build_dir, source_dir)
}

fn build_duckdb_ffi(source_dir: &Path, build_dir: &Path) {
    let use_submodule = env_or("CHRYSO_DUCKDB_USE_SUBMODULE", "ON");

    let status = Command::new("cmake")
        .arg("-S")
        .arg(source_dir)
        .arg("-B")
        .arg(build_dir)
        .arg(format!("-DCHRYSO_DUCKDB_USE_SUBMODULE={use_submodule}"))
        .arg("-DCMAKE_POLICY_VERSION_MINIMUM=3.5")
        .status()
        .unwrap_or_else(|err| panic!("failed to run cmake configure: {err}"));
    if !status.success() {
        panic!("cmake configure failed");
    }

    let mut build_cmd = Command::new("cmake");
    build_cmd.arg("--build").arg(build_dir);
    if let Ok(parallel) = env::var("CMAKE_BUILD_PARALLEL_LEVEL") {
        if !parallel.trim().is_empty() {
            build_cmd.arg("--parallel").arg(parallel);
        }
    } else {
        build_cmd.arg("--parallel");
    }
    let status = build_cmd
        .status()
        .unwrap_or_else(|err| panic!("failed to run cmake build: {err}"));
    if !status.success() {
        panic!("cmake build failed");
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn emit_rerun_if_changed(source_dir: &Path) {
    let base = source_dir.display();
    println!("cargo:rerun-if-changed={base}/CMakeLists.txt");
    println!("cargo:rerun-if-changed={base}/src");
    println!("cargo:rerun-if-changed={base}/include");
    println!("cargo:rerun-if-changed={base}/duckdb");
    println!("cargo:rerun-if-changed={base}/duckdb/CMakeLists.txt");
}
