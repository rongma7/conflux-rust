use vergen_git2::{Emitter, Git2Builder};
fn main() -> anyhow::Result<()> {
    let git2 = Git2Builder::default().all().sha(true).build()?;
    Emitter::default().add_instructions(&git2)?.emit()?;

    // Set VERGEN_RUSTC_SEMVER manually: on the storage2 rocksdb binding the
    // transitive vergen crate version has a RustcBuilder compatibility
    // mismatch, so we shell out to `rustc --version` instead.
    let output = std::process::Command::new("rustc")
        .arg("--version")
        .output()?;
    let version_str = String::from_utf8(output.stdout)?;
    let semver = version_str.split_whitespace().nth(1).unwrap_or("unknown");
    println!("cargo:rustc-env=VERGEN_RUSTC_SEMVER={}", semver);
    Ok(())
}
