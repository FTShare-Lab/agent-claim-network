//! 在编译期将可发布二进制的源码提交标识嵌入产物。

use std::env;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=ACN_GIT_COMMIT");
    println!("cargo:rerun-if-env-changed=ACN_GIT_COMMIT_TIMESTAMP");
    println!("cargo:rerun-if-env-changed=CI_COMMIT_SHA");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let raw_commit = environment_value("ACN_GIT_COMMIT")
        .or_else(|| environment_value("CI_COMMIT_SHA"))
        .or_else(git_commit);
    let (commit, commit_timestamp) = match raw_commit {
        Some(raw_commit) => {
            let commit_timestamp = environment_value("ACN_GIT_COMMIT_TIMESTAMP")
                .or_else(|| git_commit_timestamp(&raw_commit))
                .unwrap_or_else(|| {
                    if env::var_os("PROFILE").as_deref() == Some(std::ffi::OsStr::new("release")) {
                        panic!(
                            "release 构建需要 ACN_GIT_COMMIT_TIMESTAMP 或可访问的 Git commit 时间"
                        );
                    }
                    "unknown".to_string()
                });
            (short_commit(&raw_commit), commit_timestamp)
        }
        None => {
            if env::var_os("PROFILE").as_deref() == Some(std::ffi::OsStr::new("release")) {
                // 发布产物必须可追溯到源码提交，缺少此元数据时中止构建以避免发布不可识别二进制。
                panic!("release 构建需要 ACN_GIT_COMMIT 或可访问的 Git HEAD");
            }
            ("unknown".to_string(), "unknown".to_string())
        }
    };
    println!("cargo:rustc-env=ACN_GIT_COMMIT={commit}");
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

fn short_commit(commit: &str) -> String {
    commit.chars().take(7).collect()
}
