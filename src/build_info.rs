//! ACN 可执行文件共享的构建身份与版本展示。

use serde::{Deserialize, Serialize};

pub const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("ACN_GIT_COMMIT");
pub const GIT_COMMIT_FULL: &str = env!("ACN_GIT_COMMIT_FULL");
pub const GIT_COMMIT_TIMESTAMP: &str = env!("ACN_GIT_COMMIT_TIMESTAMP");

/// 用于跨进程判断两个 ACN 运行时是否来自同一份构建。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildIdentity {
    pub version: String,
    pub commit: String,
}

impl BuildIdentity {
    pub fn current() -> Self {
        Self {
            version: PACKAGE_VERSION.to_owned(),
            commit: GIT_COMMIT.to_owned(),
        }
    }

    pub fn matches_current(&self) -> bool {
        self.version == PACKAGE_VERSION && self.commit == GIT_COMMIT
    }
}

pub fn version_text(binary_name: &str) -> String {
    format!("{binary_name} {PACKAGE_VERSION} ({GIT_COMMIT}, {GIT_COMMIT_TIMESTAMP})")
}

pub fn version_requested(args: &[String]) -> bool {
    args.len() == 2 && matches!(args.get(1).map(String::as_str), Some("--version" | "-V"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_text_includes_binary_and_shared_build_metadata() {
        assert_eq!(
            version_text("acn-router"),
            format!(
                "acn-router {} ({}, {})",
                env!("CARGO_PKG_VERSION"),
                env!("ACN_GIT_COMMIT"),
                env!("ACN_GIT_COMMIT_TIMESTAMP")
            )
        );
    }

    #[test]
    fn build_identity_matches_only_same_version_and_commit() {
        assert!(BuildIdentity::current().matches_current());
        assert!(!BuildIdentity {
            version: "999.0.0".into(),
            commit: GIT_COMMIT.into(),
        }
        .matches_current());
        assert!(!BuildIdentity {
            version: PACKAGE_VERSION.into(),
            commit: "different".into(),
        }
        .matches_current());
    }

    #[test]
    fn version_flag_requires_a_single_exact_argument() {
        assert!(version_requested(&["acn".into(), "--version".into()]));
        assert!(version_requested(&["acn".into(), "-V".into()]));
        assert!(!version_requested(&["acn".into()]));
        assert!(!version_requested(&[
            "acn".into(),
            "--version".into(),
            "extra".into()
        ]));
    }
}
