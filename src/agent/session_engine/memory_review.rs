//! SessionEngine 后台 memory review 辅助。
//!
//! 本模块维护 fork memory review 的 turn cadence、最近 transcript 窗口构造
//! 与异步任务启动。review 仍只通过 `MemoryReviewLoop` 和原生 memory tool 写入，
//! 不修改主 session transcript 或 compaction 状态。

use crate::api::MemoryReviewRequest;
use crate::session::SessionHandle;

use super::transcript::{build_memory_review_transcript, memory_review_should_run};
use super::{SessionEngine, PROMPT_MEMORY_REVIEW};

impl SessionEngine {
    pub(super) fn fork_memory_review_cadence_reached(&self) -> bool {
        if !self.fork_memory_review {
            return false;
        }
        let mut turns = match self.turns_since_fork_memory_review.lock() {
            Ok(turns) => turns,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：turn 计数锁已损坏: {e}"
                );
                return false;
            }
        };
        *turns = (*turns).saturating_add(1);
        if *turns < self.fork_memory_review_interval_turns {
            return false;
        }
        *turns = 0;
        true
    }

    pub(super) fn reset_fork_memory_review_turns(&self) {
        match self.turns_since_fork_memory_review.lock() {
            Ok(mut turns) => *turns = 0,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review turn 计数重置失败：锁已损坏: {e}"
                );
            }
        }
    }

    pub(super) async fn spawn_memory_review(&self, session: &SessionHandle) {
        let system_prompt = match self.render_memory_review_system_prompt().await {
            Ok(system_prompt) => system_prompt,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：渲染 review system prompt 失败: {e}"
                );
                return;
            }
        };
        let session_messages = match session.read_messages().await {
            Ok(messages) => messages,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：读取 session transcript 失败: {e}"
                );
                return;
            }
        };
        if !memory_review_should_run(&session_messages) {
            return;
        }

        let metadata = match session.read_metadata().await {
            Ok(metadata) => metadata,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：读取 session metadata 失败: {e}"
                );
                return;
            }
        };
        let transcript = match build_memory_review_transcript(
            &metadata,
            session_messages,
            self.fork_memory_review_interval_turns,
        ) {
            Ok(transcript) => transcript,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：构造 review transcript 失败: {e}"
                );
                return;
            }
        };
        let request = MemoryReviewRequest {
            system_prompt,
            transcript,
        };

        let review_prompt = match self.prompt_registry.render(PROMPT_MEMORY_REVIEW, ()) {
            Ok(prompt) => prompt,
            Err(e) => {
                log::warn!(
                    target: "agent",
                    "background memory review 跳过：渲染 review prompt 失败: {e}"
                );
                return;
            }
        };

        let memory_review_loop = self.memory_review_loop.clone();
        let agent_id = self.runner.agent_id.clone();
        tokio::spawn(async move {
            if let Err(e) = memory_review_loop.run(request, review_prompt).await {
                log::warn!(
                    target: "agent",
                    "agent {agent_id} background memory review failed: {e}"
                );
            }
        });
    }
}
