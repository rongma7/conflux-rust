use std::env;
use std::path::Path;

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_path = Path::new(&manifest_dir);
    
    let workspace_root = manifest_path.ancestors().nth(3).unwrap();

    println!(
        "cargo:rustc-env=WORKSPACE_ROOT={}",
        workspace_root.to_str().unwrap()
    );

    println!("cargo:rerun-if-changed=build.rs");
}