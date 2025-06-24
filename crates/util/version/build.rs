use vergen_git2::{Emitter, Git2Builder};
fn main() -> anyhow::Result<()> {
    let git2 = Git2Builder::default().all().sha(true).build()?;
    Emitter::default()
        .add_instructions(&git2)?
        .emit()?;

    // Set VERGEN_RUSTC_SEMVER manually since RustcBuilder has version conflicts
    let output = std::process::Command::new("rustc")
        .arg("--version")
        .output()?;
    let version_str = String::from_utf8(output.stdout)?;
    // Parse "rustc X.Y.Z (...)" to get "X.Y.Z"
    let semver = version_str
        .split_whitespace()
        .nth(1)
        .unwrap_or("unknown");
    println!("cargo:rustc-env=VERGEN_RUSTC_SEMVER={}", semver);
    Ok(())
}
