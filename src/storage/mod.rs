//! storage 模块。
//!
//! 提供两类基础能力，所有文件 I/O 走异步：
//! - YAML 读写（`read_yaml`、`write_yaml_atomic`）
//!   原子写流程：写临时文件 → fsync → rename，避免进程崩溃留下半写状态。
//! - 路径拼接工具（`paths::*`），统一 team store / agent home 的目录约定，
//!   防止业务代码用字符串拼路径或写出"其他 agent 的本地路径"。
//! - ID 查重 + 重抽（`mint_unique_id_in_dir`），供 maintainer 写 policy 前
//!   在目录中确认候选 id 没被占用。dispute id 已改为 agent 侧派生，不再使用本 helper。
//! - 文件锁（`FileLockGuard`），供跨进程读改写同一 runtime 文件时使用。

mod file_lock;
mod jsonl;
mod mint;
pub mod paths;
mod yaml;

pub use file_lock::FileLockGuard;
pub use jsonl::{append_jsonl_record, read_jsonl_records, JsonlRotationConfig};
pub use mint::mint_unique_id_in_dir;
pub use yaml::{
    read_yaml, write_text_atomic, write_text_atomic_if_unchanged, write_yaml_atomic, StorageError,
};
