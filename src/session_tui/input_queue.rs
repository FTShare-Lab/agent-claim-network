//! TUI 输入队列状态。
//!
//! 本模块把 running turn 期间提交的后续输入集中在聊天层管理。
//! BottomPane 只负责 composer 编辑和展示，不拥有业务队列。

use std::collections::VecDeque;

use crate::api::SessionAttachment;

use super::bottom_pane::InputDraft;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PendingInputPreview {
    pub(super) queued_inputs: Vec<String>,
}

impl PendingInputPreview {
    pub(super) fn is_empty(&self) -> bool {
        self.queued_inputs.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueuedInput {
    text: String,
    draft: InputDraft,
    attachments: Vec<SessionAttachment>,
    submission_sequence: Option<u64>,
}

impl QueuedInput {
    pub(super) fn new(text: String, draft: InputDraft) -> Self {
        Self::with_extra_attachments(text, draft, Vec::new())
    }

    /// 草稿自带附件（剪贴板图片）之外再追加 `@path` 解析出的附件。
    pub(super) fn with_extra_attachments(
        text: String,
        draft: InputDraft,
        extra: Vec<SessionAttachment>,
    ) -> Self {
        let mut attachments = draft.session_attachments();
        attachments.extend(extra);
        Self {
            text,
            draft,
            attachments,
            submission_sequence: None,
        }
    }

    pub(super) fn from_text(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            text: text.clone(),
            draft: InputDraft::new(text),
            attachments: Vec::new(),
            submission_sequence: None,
        }
    }

    pub(super) fn with_submission_sequence(mut self, sequence: u64) -> Self {
        self.submission_sequence = Some(sequence);
        self
    }

    pub(super) fn text(&self) -> &str {
        &self.text
    }

    /// 用于命令及显式 Skill 识别的未展开可见输入；粘贴/图片占位符绝不参与解析。
    pub(super) fn command_text(&self) -> &str {
        self.draft.visible_text()
    }

    pub(super) fn draft(&self) -> &InputDraft {
        &self.draft
    }

    pub(super) fn attachments(&self) -> &[SessionAttachment] {
        &self.attachments
    }

    pub(super) fn submission_sequence(&self) -> Option<u64> {
        self.submission_sequence
    }

    pub(super) fn into_draft(self) -> InputDraft {
        self.draft
    }
}

impl From<String> for QueuedInput {
    fn from(text: String) -> Self {
        Self::from_text(text)
    }
}

impl From<&str> for QueuedInput {
    fn from(text: &str) -> Self {
        Self::from_text(text)
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct InputQueueState {
    queued_inputs: VecDeque<QueuedInput>,
}

impl InputQueueState {
    pub(super) fn enqueue(&mut self, input: QueuedInput) {
        self.queued_inputs.push_back(input);
    }

    pub(super) fn pop_next(&mut self) -> Option<QueuedInput> {
        self.queued_inputs.pop_front()
    }

    pub(super) fn pop_latest(&mut self) -> Option<QueuedInput> {
        self.queued_inputs.pop_back()
    }

    pub(super) fn drain_inputs_for_restore_before(
        &mut self,
        restore_before: u64,
    ) -> Vec<QueuedInput> {
        let mut restore = Vec::new();
        let mut keep = VecDeque::new();
        for input in self.queued_inputs.drain(..) {
            match input.submission_sequence() {
                Some(sequence) if sequence >= restore_before => keep.push_back(input),
                _ => restore.push(input),
            }
        }
        self.queued_inputs = keep;
        restore
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.queued_inputs.len()
    }

    pub(super) fn queued_count(&self) -> usize {
        self.queued_inputs.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queued_inputs.is_empty()
    }

    pub(super) fn preview(&self) -> PendingInputPreview {
        PendingInputPreview {
            queued_inputs: self
                .queued_inputs
                .iter()
                .map(|input| input.draft().visible_text().to_string())
                .collect(),
        }
    }

    pub(super) fn drain_for_restore(&mut self, current_draft: InputDraft) -> Option<InputDraft> {
        if self.queued_inputs.is_empty() && current_draft.is_visible_empty() {
            return None;
        }

        let mut drafts = self
            .queued_inputs
            .drain(..)
            .map(QueuedInput::into_draft)
            .collect::<Vec<_>>();
        if !current_draft.is_visible_empty() {
            drafts.push(current_draft);
        }

        merge_drafts_for_restore(drafts)
    }
}

fn merge_drafts_for_restore(drafts: Vec<InputDraft>) -> Option<InputDraft> {
    let mut iter = drafts.into_iter().filter(|draft| !draft.is_visible_empty());
    let mut restored = iter.next()?;
    for draft in iter {
        restored.append_with_newline(draft);
    }
    Some(restored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pop_next_is_fifo() {
        let mut queue = InputQueueState::default();
        queue.enqueue(QueuedInput::from_text("first"));
        queue.enqueue(QueuedInput::from_text("second"));

        assert_eq!(
            queue
                .pop_next()
                .map(|input| input.text().to_string())
                .as_deref(),
            Some("first")
        );
        assert_eq!(
            queue
                .pop_next()
                .map(|input| input.text().to_string())
                .as_deref(),
            Some("second")
        );
        assert_eq!(queue.pop_next(), None);
    }

    #[test]
    fn pop_latest_is_lifo() {
        let mut queue = InputQueueState::default();
        queue.enqueue(QueuedInput::from_text("first"));
        queue.enqueue(QueuedInput::from_text("second"));

        assert_eq!(
            queue
                .pop_latest()
                .map(|input| input.text().to_string())
                .as_deref(),
            Some("second")
        );
        assert_eq!(
            queue
                .pop_latest()
                .map(|input| input.text().to_string())
                .as_deref(),
            Some("first")
        );
        assert_eq!(queue.pop_latest(), None);
    }

    #[test]
    fn drain_for_restore_merges_queue_before_current_draft() {
        let mut queue = InputQueueState::default();
        queue.enqueue(QueuedInput::from_text("queued one"));
        queue.enqueue(QueuedInput::from_text("queued two"));

        assert_eq!(
            queue
                .drain_for_restore(InputDraft::new("draft".into()))
                .map(|draft| draft.expanded_text())
                .as_deref(),
            Some("queued one\nqueued two\ndraft")
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn drain_inputs_for_restore_before_keeps_newer_submissions_queued() {
        let mut queue = InputQueueState::default();
        queue.enqueue(QueuedInput::from_text("before").with_submission_sequence(2));
        queue.enqueue(QueuedInput::from_text("after").with_submission_sequence(3));

        let restored = queue.drain_inputs_for_restore_before(3);

        assert_eq!(
            restored
                .into_iter()
                .map(|input| input.text().to_string())
                .collect::<Vec<_>>(),
            vec!["before"]
        );
        assert_eq!(queue.queued_count(), 1);
        assert_eq!(queue.pop_next().unwrap().text(), "after");
    }
}
