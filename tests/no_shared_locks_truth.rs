//! Witness that router actor source does not share locks
//! between actors.
//!
//! Per `~/primary/skills/actor-systems.md` §"No shared locks":
//! `Arc<Mutex<T>>` / `Arc<RwLock<T>>` between actors turns the
//! lock into the real owner and makes the actors decorative.
//! The companion no-zst-actor witness lives in
//! `actor_runtime_truth.rs::public_control_records_cannot_be_zero_sized`.
//!
//! Comment lines that name the rule itself are excluded; the
//! intent is to catch executable shared-lock state, not the
//! discipline documentation.

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn actor_source_does_not_share_locks_between_actors() {
    let forbidden = [
        ("Arc<Mutex", "shared mutex state between actors"),
        ("Arc < Mutex", "shared mutex state between actors"),
        ("RwLock", "shared read-write lock state between actors"),
    ];

    let mut violations: Vec<String> = Vec::new();
    for path in production_source_files() {
        let text = fs::read_to_string(&path).expect("read source file");
        for (fragment, reason) in forbidden {
            for (index, line) in text.lines().enumerate() {
                if !line.contains(fragment) {
                    continue;
                }
                let trimmed = line.trim_start();
                if trimmed.starts_with("//") {
                    continue;
                }
                violations.push(format!(
                    "{}:{}: {reason} ({line})",
                    path.display(),
                    index + 1,
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "shared-lock violations in actor source:\n{}",
        violations.join("\n"),
    );
}

fn production_source_files() -> Vec<PathBuf> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = crate_root.join("src");
    let mut output = Vec::new();
    collect_rust_files(&src, &mut output);
    output
}

fn collect_rust_files(directory: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}
