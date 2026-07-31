//! 旧版单 upstream 本地目录到 selected-upstream runtime 的自动迁移。
//!
//! 合入 upstream 隔离后，`<acn_home>` 只保留全局入口，运行时状态进入
//! `<acn_home>/<upstream>/`。本模块只处理首次启动时的 Agent 私有目录搬迁；
//! Router / Maintainer 使用的 `<acn_home>/data/team` 始终留在 daemon 数据根。

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

use crate::claim::AgentId;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LegacyRuntimeMigrationReport {
    pub moved: usize,
    pub skipped_missing: usize,
    pub skipped_empty: usize,
    pub skipped_existing_target: usize,
}

#[derive(Debug, Error)]
pub enum LegacyRuntimeMigrationError {
    #[error("旧 ACN 目录迁移路径越过 acn_home: {path}")]
    PathEscapesAcnHome { path: PathBuf },
    #[error("旧 ACN 目录迁移源不能是 symlink: {path}")]
    SourceSymlink { path: PathBuf },
    #[error("旧 ACN 目录迁移源的父目录不能包含 symlink: {path}")]
    SourceAncestorSymlink { path: PathBuf },
    #[error("旧 ACN 目录迁移源目录不能包含 symlink: {path}")]
    SourceTreeSymlink { path: PathBuf },
    #[error("旧 ACN 目录迁移源必须是普通文件或目录: {path}")]
    UnsupportedSource { path: PathBuf },
    #[error("旧 ACN 目录迁移目标父目录不能包含 symlink: {path}")]
    TargetParentSymlink { path: PathBuf },
    #[error("旧 ACN 目录迁移 {action} 失败: {path} ({source})")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

struct MigrationLock {
    file: File,
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        if let Err(err) = self.file.unlock() {
            log::warn!(target: "upstream_migration", "释放迁移文件锁失败: {err}");
        }
    }
}

struct MigrationItem {
    src: PathBuf,
    dst: PathBuf,
}

/// 将旧 `<acn_home>` 下已存在的运行时状态迁移到 `<acn_home>/<upstream>/`。
///
/// 迁移只移动 Agent 的 legacy runtime 文件，不迁移 `config.toml` 或 daemon 的
/// `data/team`。目标已存在时跳过，避免覆盖 upstream runtime 下的新状态。
pub fn migrate_legacy_runtime_if_needed(
    base_acn_home: &Path,
    upstream_name: &str,
    agent_id: &AgentId,
) -> Result<LegacyRuntimeMigrationReport, LegacyRuntimeMigrationError> {
    let target_root = base_acn_home.join(upstream_name);
    let items = migration_items(base_acn_home, &target_root, agent_id);
    if !items.iter().any(|item| lstat_exists(&item.src)) {
        return Ok(LegacyRuntimeMigrationReport::default());
    }

    let _lock = lock_migration(base_acn_home)?;
    let mut report = LegacyRuntimeMigrationReport::default();
    for item in migration_items(base_acn_home, &target_root, agent_id) {
        migrate_item(base_acn_home, &item, &mut report)?;
    }
    remove_empty_legacy_parents(base_acn_home);
    Ok(report)
}

fn migration_items(
    base_acn_home: &Path,
    target_root: &Path,
    agent_id: &AgentId,
) -> Vec<MigrationItem> {
    vec![
        MigrationItem {
            src: base_acn_home.join(".mcp.json"),
            dst: target_root.join(".mcp.json"),
        },
        MigrationItem {
            src: base_acn_home.join("ACN.md"),
            dst: target_root.join("ACN.md"),
        },
        MigrationItem {
            src: base_acn_home.join("skills"),
            dst: target_root.join("skills"),
        },
        MigrationItem {
            src: base_acn_home
                .join("data")
                .join("agents")
                .join(agent_id.as_str()),
            dst: target_root
                .join("data")
                .join("agents")
                .join(agent_id.as_str()),
        },
    ]
}

fn lock_migration(base_acn_home: &Path) -> Result<MigrationLock, LegacyRuntimeMigrationError> {
    fs::create_dir_all(base_acn_home).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "创建 acn_home",
        path: base_acn_home.to_path_buf(),
        source,
    })?;
    let lock_path = base_acn_home.join(".upstream_migration.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "打开迁移锁",
            path: lock_path.clone(),
            source,
        })?;
    file.lock_exclusive()
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "获取迁移锁",
            path: lock_path,
            source,
        })?;
    Ok(MigrationLock { file })
}

fn migrate_item(
    base_acn_home: &Path,
    item: &MigrationItem,
    report: &mut LegacyRuntimeMigrationReport,
) -> Result<(), LegacyRuntimeMigrationError> {
    if !lstat_exists(&item.src) {
        report.skipped_missing += 1;
        return Ok(());
    }
    ensure_under(item.src.as_path(), base_acn_home)?;
    ensure_under(item.dst.as_path(), base_acn_home)?;
    reject_source_ancestor_symlinks(item.src.as_path(), base_acn_home)?;
    reject_source_symlinks(item.src.as_path())?;
    if !source_has_content(item.src.as_path())? {
        report.skipped_empty += 1;
        return Ok(());
    }
    if lstat_exists(&item.dst) {
        report.skipped_existing_target += 1;
        return Ok(());
    }

    ensure_target_parent(item.dst.as_path(), base_acn_home)?;
    copy_item_no_overwrite(item.src.as_path(), item.dst.as_path())?;
    remove_source(item.src.as_path())?;
    report.moved += 1;
    Ok(())
}

fn ensure_under(path: &Path, root: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    if path.starts_with(root) {
        return Ok(());
    }
    Err(LegacyRuntimeMigrationError::PathEscapesAcnHome {
        path: path.to_path_buf(),
    })
}

fn lstat_exists(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

fn reject_source_ancestor_symlinks(
    path: &Path,
    base_acn_home: &Path,
) -> Result<(), LegacyRuntimeMigrationError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let relative = parent.strip_prefix(base_acn_home).map_err(|_| {
        LegacyRuntimeMigrationError::PathEscapesAcnHome {
            path: parent.to_path_buf(),
        }
    })?;
    let mut current = base_acn_home.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        if current
            .symlink_metadata()
            .is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Err(LegacyRuntimeMigrationError::SourceAncestorSymlink { path: current });
        }
    }
    Ok(())
}

fn reject_source_symlinks(path: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    let metadata = path
        .symlink_metadata()
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "读取源元数据",
            path: path.to_path_buf(),
            source,
        })?;
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(LegacyRuntimeMigrationError::SourceSymlink {
            path: path.to_path_buf(),
        });
    }
    if file_type.is_file() {
        return Ok(());
    }
    if !file_type.is_dir() {
        return Err(LegacyRuntimeMigrationError::UnsupportedSource {
            path: path.to_path_buf(),
        });
    }
    reject_source_tree_symlinks(path)
}

fn reject_source_tree_symlinks(path: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    for entry in fs::read_dir(path).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "读取源目录",
        path: path.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "遍历源目录",
            path: path.to_path_buf(),
            source,
        })?;
        let child_path = entry.path();
        let metadata =
            child_path
                .symlink_metadata()
                .map_err(|source| LegacyRuntimeMigrationError::Io {
                    action: "读取源子项元数据",
                    path: child_path.clone(),
                    source,
                })?;
        let file_type = metadata.file_type();
        if file_type.is_symlink() {
            return Err(LegacyRuntimeMigrationError::SourceTreeSymlink { path: child_path });
        }
        if file_type.is_dir() {
            reject_source_tree_symlinks(&child_path)?;
        } else if !file_type.is_file() {
            return Err(LegacyRuntimeMigrationError::UnsupportedSource { path: child_path });
        }
    }
    Ok(())
}

fn source_has_content(path: &Path) -> Result<bool, LegacyRuntimeMigrationError> {
    let metadata = path
        .symlink_metadata()
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "读取源元数据",
            path: path.to_path_buf(),
            source,
        })?;
    if metadata.is_file() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(path).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "读取源目录",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(entries.next().is_some())
}

fn ensure_target_parent(
    dst: &Path,
    base_acn_home: &Path,
) -> Result<(), LegacyRuntimeMigrationError> {
    let Some(parent) = dst.parent() else {
        return Ok(());
    };
    reject_target_parent_symlinks(parent, base_acn_home)?;
    fs::create_dir_all(parent).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "创建目标父目录",
        path: parent.to_path_buf(),
        source,
    })?;
    reject_target_parent_symlinks(parent, base_acn_home)
}

fn reject_target_parent_symlinks(
    parent: &Path,
    base_acn_home: &Path,
) -> Result<(), LegacyRuntimeMigrationError> {
    let relative = parent.strip_prefix(base_acn_home).map_err(|_| {
        LegacyRuntimeMigrationError::PathEscapesAcnHome {
            path: parent.to_path_buf(),
        }
    })?;
    let mut current = base_acn_home.to_path_buf();
    for part in relative.components() {
        current.push(part.as_os_str());
        if current
            .symlink_metadata()
            .is_ok_and(|meta| meta.file_type().is_symlink())
        {
            return Err(LegacyRuntimeMigrationError::TargetParentSymlink { path: current });
        }
    }
    Ok(())
}

fn copy_item_no_overwrite(src: &Path, dst: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    let metadata = src
        .symlink_metadata()
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "读取源元数据",
            path: src.to_path_buf(),
            source,
        })?;
    if metadata.is_dir() {
        copy_dir_no_overwrite(src, dst)
    } else {
        copy_file_no_overwrite(src, dst)
    }
}

fn copy_dir_no_overwrite(src: &Path, dst: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    fs::create_dir(dst).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "创建目标目录",
        path: dst.to_path_buf(),
        source,
    })?;
    let result = copy_dir_contents(src, dst).and_then(|()| {
        let metadata = fs::metadata(src).map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "读取源目录权限",
            path: src.to_path_buf(),
            source,
        })?;
        fs::set_permissions(dst, metadata.permissions()).map_err(|source| {
            LegacyRuntimeMigrationError::Io {
                action: "设置目标目录权限",
                path: dst.to_path_buf(),
                source,
            }
        })
    });
    if result.is_err() {
        let _ = remove_item(dst);
    }
    result
}

fn copy_dir_contents(src: &Path, dst: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    for entry in fs::read_dir(src).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "读取源目录",
        path: src.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "遍历源目录",
            path: src.to_path_buf(),
            source,
        })?;
        let child_src = entry.path();
        let child_dst = dst.join(entry.file_name());
        let metadata =
            child_src
                .symlink_metadata()
                .map_err(|source| LegacyRuntimeMigrationError::Io {
                    action: "读取源子项元数据",
                    path: child_src.clone(),
                    source,
                })?;
        if metadata.is_dir() {
            copy_dir_no_overwrite(&child_src, &child_dst)?;
        } else {
            copy_file_no_overwrite(&child_src, &child_dst)?;
        }
    }
    Ok(())
}

fn copy_file_no_overwrite(src: &Path, dst: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    let mut source = File::open(src).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "打开源文件",
        path: src.to_path_buf(),
        source,
    })?;
    let mut target = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)
        .map_err(|source| LegacyRuntimeMigrationError::Io {
            action: "创建目标文件",
            path: dst.to_path_buf(),
            source,
        })?;
    io::copy(&mut source, &mut target).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "复制文件",
        path: dst.to_path_buf(),
        source,
    })?;
    let metadata = fs::metadata(src).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "读取源文件权限",
        path: src.to_path_buf(),
        source,
    })?;
    fs::set_permissions(dst, metadata.permissions()).map_err(|source| {
        LegacyRuntimeMigrationError::Io {
            action: "设置目标文件权限",
            path: dst.to_path_buf(),
            source,
        }
    })?;
    Ok(())
}

fn remove_source(src: &Path) -> Result<(), LegacyRuntimeMigrationError> {
    remove_item(src).map_err(|source| LegacyRuntimeMigrationError::Io {
        action: "删除旧源",
        path: src.to_path_buf(),
        source,
    })
}

fn remove_item(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn remove_empty_legacy_parents(base_acn_home: &Path) {
    for path in [
        base_acn_home.join("data").join("agents"),
        base_acn_home.join("data"),
    ] {
        let _ = fs::remove_dir(path);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::tempdir;

    use super::*;

    fn agent_a() -> AgentId {
        AgentId::new("agent-a".to_string()).unwrap()
    }

    #[test]
    fn migrates_legacy_runtime_items_without_config_toml() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(home.join("skills").join("sample")).unwrap();
        fs::create_dir_all(
            home.join("data")
                .join("agents")
                .join("agent-a")
                .join("sessions"),
        )
        .unwrap();
        fs::create_dir_all(
            home.join("data")
                .join("team")
                .join("maintainer")
                .join("policies"),
        )
        .unwrap();
        fs::write(home.join("config.toml"), "upstream = \"dev\"\n").unwrap();
        fs::write(home.join(".mcp.json"), "{\"mcpServers\":{}}\n").unwrap();
        fs::write(home.join("ACN.md"), "prefer concise answers\n").unwrap();
        fs::write(
            home.join("skills").join("sample").join("SKILL.md"),
            "# sample\n",
        )
        .unwrap();
        fs::write(
            home.join("data")
                .join("agents")
                .join("agent-a")
                .join("sessions")
                .join("session.yaml"),
            "id: session_1234abcd\n",
        )
        .unwrap();
        fs::write(
            home.join("data")
                .join("team")
                .join("maintainer")
                .join("policies")
                .join("policy.yaml"),
            "id: policy_1234abcd\n",
        )
        .unwrap();
        let report = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap();

        assert_eq!(report.moved, 4);
        assert!(home.join("config.toml").is_file());
        assert!(!home.join(".mcp.json").exists());
        assert!(!home.join("ACN.md").exists());
        assert!(!home.join("skills").exists());
        assert!(!home.join("data").join("agents").join("agent-a").exists());
        assert!(home.join("data").join("team").exists());
        assert_eq!(
            fs::read_to_string(home.join("dev").join(".mcp.json")).unwrap(),
            "{\"mcpServers\":{}}\n"
        );
        assert!(home.join("dev").join("ACN.md").is_file());
        assert!(home
            .join("dev")
            .join("skills")
            .join("sample")
            .join("SKILL.md")
            .is_file());
        assert!(home
            .join("dev")
            .join("data")
            .join("agents")
            .join("agent-a")
            .join("sessions")
            .join("session.yaml")
            .is_file());
        assert!(!home
            .join("dev")
            .join("data")
            .join("team")
            .join("maintainer")
            .join("policies")
            .join("policy.yaml")
            .exists());
    }

    #[test]
    fn leaves_daemon_team_storage_in_base_acn_home() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        let claim_path = home
            .join("data")
            .join("team")
            .join("agents")
            .join("agent-a")
            .join("claims")
            .join("claim.yaml");
        fs::create_dir_all(claim_path.parent().unwrap()).unwrap();
        fs::write(&claim_path, "id: claim_1234abcd\n").unwrap();

        let report = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap();

        assert_eq!(report, LegacyRuntimeMigrationReport::default());
        assert_eq!(
            fs::read_to_string(&claim_path).unwrap(),
            "id: claim_1234abcd\n"
        );
        assert!(!home.join("dev").join("data").join("team").exists());
    }

    #[test]
    fn keeps_legacy_source_when_target_exists() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(home.join("dev")).unwrap();
        fs::write(home.join(".mcp.json"), "legacy\n").unwrap();
        fs::write(home.join("dev").join(".mcp.json"), "new\n").unwrap();

        let report = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap();

        assert_eq!(report.moved, 0);
        assert_eq!(report.skipped_existing_target, 1);
        assert_eq!(
            fs::read_to_string(home.join(".mcp.json")).unwrap(),
            "legacy\n"
        );
        assert_eq!(
            fs::read_to_string(home.join("dev").join(".mcp.json")).unwrap(),
            "new\n"
        );
    }

    #[test]
    fn skips_empty_agent_runtime_directories_and_ignores_team_storage() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(home.join("skills")).unwrap();
        fs::create_dir_all(home.join("data").join("team")).unwrap();

        let report = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap();

        assert_eq!(report.moved, 0);
        assert_eq!(report.skipped_empty, 1);
        assert!(home.join("skills").is_dir());
        assert!(home.join("data").join("team").is_dir());
        assert!(!home.join("dev").exists());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_source_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(&home).unwrap();
        fs::write(dir.path().join("outside.json"), "{}\n").unwrap();
        symlink(dir.path().join("outside.json"), home.join(".mcp.json")).unwrap();

        let err = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap_err();

        assert!(matches!(
            err,
            LegacyRuntimeMigrationError::SourceSymlink { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_inside_source_tree() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(home.join("skills")).unwrap();
        fs::write(dir.path().join("outside.md"), "# outside\n").unwrap();
        symlink(
            dir.path().join("outside.md"),
            home.join("skills").join("SKILL.md"),
        )
        .unwrap();

        let err = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap_err();

        assert!(matches!(
            err,
            LegacyRuntimeMigrationError::SourceTreeSymlink { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_in_source_ancestor_without_touching_team_storage() {
        use std::os::unix::fs::symlink;

        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        let team_agents = home.join("data").join("team").join("agents");
        let claim = team_agents
            .join("agent-a")
            .join("claims")
            .join("claim.yaml");
        fs::create_dir_all(claim.parent().unwrap()).unwrap();
        fs::write(&claim, "id: claim_1234abcd\n").unwrap();
        symlink(&team_agents, home.join("data").join("agents")).unwrap();

        let err = migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap_err();

        assert!(matches!(
            err,
            LegacyRuntimeMigrationError::SourceAncestorSymlink { .. }
        ));
        assert_eq!(fs::read_to_string(&claim).unwrap(), "id: claim_1234abcd\n");
        assert!(!home.join("dev").exists());
    }

    #[test]
    fn copied_files_are_readable_before_source_removal() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("acn");
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join("ACN.md"), "hello\n").unwrap();

        migrate_legacy_runtime_if_needed(&home, "dev", &agent_a()).unwrap();

        let mut text = String::new();
        File::open(home.join("dev").join("ACN.md"))
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(text, "hello\n");
    }
}
