//! ID 占位 + 重抽工具：用 `O_CREAT | O_EXCL` 在目标目录原子创建一个 0 字节
//! 占位文件 `<id>.yaml` 来"申领"该 id；若文件已存在则重抽。
//!
//! 用例：maintainer 写 policy 前先调 `mint_unique_id_in_dir` 拿到 id，
//! 调用方随后用 `write_yaml_atomic` 写真实内容（其内部 rename 会覆盖占位文件）。
//! 注：dispute id 已改为 agent 侧派生（见 `claim/id.rs::DisputeId::from_dispute_parts`），
//! 不再走本 helper。
//!
//! 设计要点：
//! - "查重 + 占位"被合并为一次 `create_new(true)` 系统调用；不再有 TOCTOU 窗口
//! - 同一进程内连续 mint 也能正确去重（前一次的占位让后一次撞上 AlreadyExists）
//! - 进程在 mint 与真实写入之间崩溃会留下 0 字节孤儿文件；它仅占用一个 id 不会被复用，
//!   但后续可考虑加一个清理任务回收
//! - I/O 错误（权限、路径异常等）一律向上传播；不再用 `unwrap_or(false)` 把错误降级成"不存在"

use std::fmt::Display;
use std::io::ErrorKind;
use std::path::Path;

use tokio::fs::{self, OpenOptions};

/// 在目录 `dir` 中以 `O_CREAT | O_EXCL` 原子创建 `<id>.yaml` 占位文件。
/// 最多尝试 `max_attempts` 次（包含首次）。目录不存在会自动创建。
///
/// `factory` 每次调用返回一个新的随机候选 id；若占位写撞上已有文件
/// (`ErrorKind::AlreadyExists`) 则继续重抽，直到成功或耗尽尝试次数。
/// 其他 I/O 错误（权限、文件系统异常等）直接 bail。
pub async fn mint_unique_id_in_dir<F, Id>(
    dir: &Path,
    mut factory: F,
    max_attempts: usize,
) -> anyhow::Result<Id>
where
    F: FnMut() -> Id,
    Id: Display,
{
    if max_attempts == 0 {
        anyhow::bail!("mint_unique_id_in_dir: max_attempts 必须 >= 1");
    }
    // 占位写要求父目录存在；先建好，承接老接口"目录不存在视为空"的语义
    fs::create_dir_all(dir)
        .await
        .map_err(|e| anyhow::anyhow!("mint_unique_id_in_dir: 创建目录 {dir:?} 失败: {e}"))?;
    let mut last_collision: Option<String> = None;
    for _ in 0..max_attempts {
        let candidate = factory();
        let path = dir.join(format!("{candidate}.yaml"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(_file) => {
                // 0 字节占位写入即视为持有该 id slot；调用方稍后 rename 覆盖即可
                return Ok(candidate);
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                last_collision = Some(candidate.to_string());
                continue;
            }
            Err(e) => {
                anyhow::bail!("mint_unique_id_in_dir: 占位写入失败 path={path:?}: {e}");
            }
        }
    }
    anyhow::bail!(
        "mint_unique_id_in_dir: 尝试 {max_attempts} 次仍碰撞（最近一次候选 id={}），目录={:?}",
        last_collision.as_deref().unwrap_or("?"),
        dir
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::PolicyId;
    use std::cell::Cell;

    #[tokio::test]
    async fn returns_first_candidate_when_dir_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("never_existed");
        let id = PolicyId::random();
        let want = id.clone();
        let got: PolicyId = mint_unique_id_in_dir(&dir, || want.clone(), 4)
            .await
            .unwrap();
        assert_eq!(got, id);
        // 目录被自动创建，占位文件落地
        assert!(dir.join(format!("{id}.yaml")).exists());
    }

    #[tokio::test]
    async fn returns_first_candidate_when_dir_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let id = PolicyId::random();
        let want = id.clone();
        let got: PolicyId = mint_unique_id_in_dir(tmp.path(), || want.clone(), 4)
            .await
            .unwrap();
        assert_eq!(got, id);
        assert!(tmp.path().join(format!("{id}.yaml")).exists());
    }

    #[tokio::test]
    async fn mints_writes_zero_byte_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        let id = PolicyId::random();
        let want = id.clone();
        let got: PolicyId = mint_unique_id_in_dir(tmp.path(), || want.clone(), 1)
            .await
            .unwrap();
        let path = tmp.path().join(format!("{got}.yaml"));
        let meta = tokio::fs::metadata(&path).await.unwrap();
        assert_eq!(meta.len(), 0, "占位文件应当是 0 字节");
    }

    #[tokio::test]
    async fn retries_on_collision_until_unique() {
        let tmp = tempfile::tempdir().unwrap();
        let collide = PolicyId::random();
        let unique = PolicyId::random();
        // 预先种一个会撞名的文件
        let collide_path = tmp.path().join(format!("{collide}.yaml"));
        tokio::fs::write(&collide_path, "x").await.unwrap();

        let calls = Cell::new(0usize);
        let factory = || {
            let n = calls.get();
            calls.set(n + 1);
            if n == 0 {
                collide.clone()
            } else {
                unique.clone()
            }
        };
        let got: PolicyId = mint_unique_id_in_dir(tmp.path(), factory, 4).await.unwrap();
        assert_eq!(got, unique);
        assert_eq!(calls.get(), 2, "第一次撞名后应重抽一次");
    }

    /// 同进程内连续两次 mint 同一候选：第一次占位后，第二次必须看到 AlreadyExists 而重抽。
    /// 这是 #5 在 #1 修好后自动消失的核心保证。
    #[tokio::test]
    async fn second_mint_with_same_candidate_is_blocked_by_first_placeholder() {
        let tmp = tempfile::tempdir().unwrap();
        let collide = PolicyId::random();
        let unique = PolicyId::random();

        let first: PolicyId = mint_unique_id_in_dir(tmp.path(), || collide.clone(), 1)
            .await
            .unwrap();
        assert_eq!(first, collide);

        // 第二次 mint factory：先返回同一 collide（应被占位文件挡住），再返回 unique
        let calls = Cell::new(0usize);
        let factory = || {
            let n = calls.get();
            calls.set(n + 1);
            if n == 0 {
                collide.clone()
            } else {
                unique.clone()
            }
        };
        let second: PolicyId = mint_unique_id_in_dir(tmp.path(), factory, 4).await.unwrap();
        assert_eq!(second, unique);
        assert_eq!(calls.get(), 2);
    }

    #[tokio::test]
    async fn bails_after_max_attempts_all_collide() {
        let tmp = tempfile::tempdir().unwrap();
        let collide = PolicyId::random();
        tokio::fs::write(tmp.path().join(format!("{collide}.yaml")), "x")
            .await
            .unwrap();

        let factory = || collide.clone();
        let err = mint_unique_id_in_dir::<_, PolicyId>(tmp.path(), factory, 4)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("尝试 4 次仍碰撞"));
    }

    #[tokio::test]
    async fn zero_max_attempts_bails() {
        let tmp = tempfile::tempdir().unwrap();
        let factory = PolicyId::random;
        let err = mint_unique_id_in_dir::<_, PolicyId>(tmp.path(), factory, 0)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(">= 1"));
    }
}
