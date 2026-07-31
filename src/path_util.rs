//! 路径文本规范化的共享辅助函数。
//!
//! 当前只处理当前用户的 home 目录缩写，供配置加载与本地工具在进入各自
//! 路径解析流程前复用；不会解释 shell 变量、glob 或其他用户的 `~name`。

use std::path::{Path, PathBuf};

/// 展开当前用户的 `~` 或 `~/...` 路径；无法确定 home 时保留原路径。
pub(crate) fn expand_current_user_home(path: &Path) -> PathBuf {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    expand_current_user_home_with(path, home.as_deref())
}

/// 使用调用方提供的 home 展开路径，便于保持路径解析逻辑可测试。
pub(crate) fn expand_current_user_home_with(path: &Path, home: Option<&Path>) -> PathBuf {
    let Some(raw) = path.to_str() else {
        return path.to_path_buf();
    };
    let Some(rest) = raw.strip_prefix('~') else {
        return path.to_path_buf();
    };
    let Some(home) = home else {
        return path.to_path_buf();
    };
    if rest.is_empty() {
        return home.to_path_buf();
    }
    let Some(relative) = rest.strip_prefix('/') else {
        return path.to_path_buf();
    };

    // `Path::join` 遇到绝对右值会丢弃左侧 home；连续 `/` 必须先消掉。
    home.join(relative.trim_start_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_only_current_user_home_notation() {
        let home = Path::new("/tmp/acn-home");

        assert_eq!(
            expand_current_user_home_with(Path::new("~"), Some(home)),
            home
        );
        assert_eq!(
            expand_current_user_home_with(Path::new("~/work/note.txt"), Some(home)),
            home.join("work/note.txt")
        );
        assert_eq!(
            expand_current_user_home_with(Path::new("~//work/note.txt"), Some(home)),
            home.join("work/note.txt")
        );
        assert_eq!(
            expand_current_user_home_with(Path::new("~other/work"), Some(home)),
            PathBuf::from("~other/work")
        );
        assert_eq!(
            expand_current_user_home_with(Path::new("relative/work"), Some(home)),
            PathBuf::from("relative/work")
        );
    }

    #[test]
    fn preserves_tilde_path_when_home_is_unavailable() {
        assert_eq!(
            expand_current_user_home_with(Path::new("~/work"), None),
            PathBuf::from("~/work")
        );
    }
}
