//! Maintainer 对外 client 边界。
//!
//! Agent 只依赖这里定义的协议 trait；bootstrap 可装配本地 adapter 或 HTTP client，
//! 业务编排不感知具体 transport。

use async_trait::async_trait;

use crate::claim::{AgentId, Claim, Dispute, InboxId, InboxMessage};

#[derive(Debug, Clone, thiserror::Error)]
pub enum MaintainerClientError {
    #[error("maintainer {operation} 鉴权失败: status={status}")]
    Auth { operation: String, status: u16 },
    #[error(
        "maintainer {operation} 暂时不可用: {detail}",
        detail = retryable_error_detail(.timed_out, .timeout_secs, .message)
    )]
    Retryable {
        operation: String,
        timeout_secs: u64,
        timed_out: bool,
        message: String,
    },
    #[error("maintainer {operation} 客户端错误: status={status} body={body}")]
    Client {
        operation: String,
        status: u16,
        body: String,
    },
    #[error("maintainer {operation} 在旧版服务上不可用: status={status}")]
    LegacyServer { operation: String, status: u16 },
}

fn retryable_error_detail(timed_out: &bool, timeout_secs: &u64, message: &str) -> String {
    if *timed_out {
        format!("timeout={timeout_secs}s {message}")
    } else {
        message.to_owned()
    }
}

impl MaintainerClientError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }

    pub fn is_auth(&self) -> bool {
        matches!(self, Self::Auth { .. })
    }

    pub fn is_legacy_server(&self) -> bool {
        matches!(self, Self::LegacyServer { .. })
    }

    pub fn timeout_secs(&self) -> Option<u64> {
        match self {
            Self::Retryable { timeout_secs, .. } => Some(*timeout_secs),
            Self::Auth { .. } | Self::Client { .. } | Self::LegacyServer { .. } => None,
        }
    }

    pub fn timed_out(&self) -> bool {
        matches!(
            self,
            Self::Retryable {
                timed_out: true,
                ..
            }
        )
    }
}

pub fn is_maintainer_auth_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<MaintainerClientError>()
        .is_some_and(MaintainerClientError::is_auth)
}

#[async_trait]
pub trait MaintainerClient: Send + Sync {
    /// Agent session 启动前 pull 自己应收的 inbox 消息；首次 pull 自动 lazy register。
    async fn pull_inbox(&self, agent_id: &AgentId) -> anyhow::Result<Vec<InboxMessage>>;
    /// 确认 inbox 消息已经持久写入 Agent 本地存储。
    async fn ack_inbox(&self, agent_id: &AgentId, inbox_ids: &[InboxId]) -> anyhow::Result<()>;
    /// Best-effort 上传 claim；调用侧会把 401/403 降级为不自动重试的 warning，
    /// timeout / retryable 失败保留 pending 重试。
    async fn upload_claim(&self, claim: &Claim) -> anyhow::Result<()>;
    /// Best-effort 上报 dispute；调用侧会把 401/403 降级为不自动重试的 warning，
    /// timeout / retryable 失败保留 pending 重试。
    async fn report_dispute(&self, dispute: &Dispute) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// best-effort 上传/上报只对 `Auth` 降级，其余错误（超时/可重试/客户端/IO）必须照常上抛。
    /// 这里锁住该分类语义，避免有人扩大 `is_maintainer_auth_error` 把真实故障也吞掉。
    #[test]
    fn auth_error_detected_only_for_auth_variant() {
        let auth: anyhow::Error = MaintainerClientError::Auth {
            operation: "POST claims/upload".into(),
            status: 401,
        }
        .into();
        assert!(is_maintainer_auth_error(&auth));

        let retryable: anyhow::Error = MaintainerClientError::Retryable {
            operation: "POST claims/upload".into(),
            timeout_secs: 20,
            timed_out: false,
            message: "x".into(),
        }
        .into();
        assert!(!is_maintainer_auth_error(&retryable));

        let client: anyhow::Error = MaintainerClientError::Client {
            operation: "POST claims/upload".into(),
            status: 400,
            body: "x".into(),
        }
        .into();
        assert!(!is_maintainer_auth_error(&client));

        let legacy: anyhow::Error = MaintainerClientError::LegacyServer {
            operation: "POST inbox/ack".into(),
            status: 404,
        }
        .into();
        assert!(!is_maintainer_auth_error(&legacy));
        assert!(legacy
            .downcast_ref::<MaintainerClientError>()
            .is_some_and(MaintainerClientError::is_legacy_server));

        // 普通 anyhow 错误（如超时 / IO）不应被误判为 auth。
        assert!(!is_maintainer_auth_error(&anyhow::anyhow!(
            "connection timed out"
        )));
    }

    #[test]
    fn retryable_error_only_displays_timeout_for_actual_timeout() {
        let response_error = MaintainerClientError::Retryable {
            operation: "POST /inbox/pull".into(),
            timeout_secs: 30,
            timed_out: false,
            message: "status=502 body=None".into(),
        };
        assert_eq!(
            response_error.to_string(),
            "maintainer POST /inbox/pull 暂时不可用: status=502 body=None"
        );

        let timeout_error = MaintainerClientError::Retryable {
            operation: "POST /inbox/pull".into(),
            timeout_secs: 30,
            timed_out: true,
            message: "request deadline exceeded".into(),
        };
        assert_eq!(
            timeout_error.to_string(),
            "maintainer POST /inbox/pull 暂时不可用: timeout=30s request deadline exceeded"
        );
    }
}
