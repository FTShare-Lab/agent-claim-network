//! 在编译期将可发布二进制的源码提交标识嵌入产物。

use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=ACN_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=ACN_GIT_COMMIT_TIMESTAMP");
    println!("cargo:rerun-if-env-changed=CI_COMMIT_SHA");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");
    emit_git_rerun_path("HEAD");
    emit_git_rerun_path("index");

    let release = env::var_os("PROFILE").as_deref() == Some(std::ffi::OsStr::new("release"));
    if release && git_worktree_dirty() == Some(true) {
        panic!("release 构建拒绝包含未提交或未跟踪源码的工作树");
    }

    let raw_commit = environment_value("ACN_GIT_COMMIT")
        .or_else(|| environment_value("CI_COMMIT_SHA"))
        .or_else(git_commit);
    let (commit, full_commit, commit_timestamp) = match raw_commit {
        Some(raw_commit) => {
            let full_commit = normalized_full_commit(&raw_commit).unwrap_or_else(|| {
                if release {
                    panic!("release 构建需要完整的 40 位 Git commit");
                }
                "unknown".to_string()
            });
            let commit_timestamp = environment_value("ACN_GIT_COMMIT_TIMESTAMP")
                .or_else(|| git_commit_timestamp(&raw_commit))
                .unwrap_or_else(|| {
                    if release {
                        panic!(
                            "release 构建需要 ACN_GIT_COMMIT_TIMESTAMP 或可访问的 Git commit 时间"
                        );
                    }
                    "unknown".to_string()
                });
            (short_commit(&raw_commit), full_commit, commit_timestamp)
        }
        None => {
            if release {
                // 发布产物必须可追溯到源码提交，缺少此元数据时中止构建以避免发布不可识别二进制。
                panic!("release 构建需要 ACN_GIT_COMMIT 或可访问的 Git HEAD");
            }
            (
                "unknown".to_string(),
                "unknown".to_string(),
                "unknown".to_string(),
            )
        }
    };
    println!("cargo:rustc-env=ACN_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=ACN_GIT_COMMIT_FULL={full_commit}");
    println!("cargo:rustc-env=ACN_GIT_COMMIT_TIMESTAMP={commit_timestamp}");
}

fn environment_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_worktree_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=normal"])
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

fn emit_git_rerun_path(name: &str) {
    let output = match Command::new("git")
        .args(["rev-parse", "--git-path", name])
        .output()
    {
        Ok(output) if output.status.success() => output,
        _ => return,
    };
    if let Ok(path) = String::from_utf8(output.stdout) {
        let path = path.trim();
        if !path.is_empty() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

fn git_commit_timestamp(commit: &str) -> Option<String> {
    let output = Command::new("git")
        .args([
            "show",
            "-s",
            "--date=format:%Y-%m-%d %H:%M:%S",
            "--format=%cd",
            commit,
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalized_full_commit(commit: &str) -> Option<String> {
    let normalized = commit.trim().to_ascii_lowercase();
    if normalized.len() == 40
        && normalized
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Some(normalized);
    }
    let revision = format!("{commit}^{{commit}}");
    let output = Command::new("git")
        .args(["rev-parse", &revision])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let resolved = String::from_utf8(output.stdout)
        .ok()?
        .trim()
        .to_ascii_lowercase();
    (resolved.len() == 40
        && resolved
            .chars()
            .all(|character| character.is_ascii_hexdigit()))
    .then_some(resolved)
}

fn short_commit(commit: &str) -> String {
    commit.chars().take(7).collect()
}
