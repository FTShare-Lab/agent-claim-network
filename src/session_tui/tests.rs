use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::style::{Color, Modifier};
use ratatui::text::Line;
use unicode_width::UnicodeWidthStr;

use crate::agent::{
    SessionEvent, SessionRuntimeStatus, TeamServiceConnectionStatus, TeamServicesConnectionStatus,
    UserShellCommandStatus,
};
use crate::api::{ToolCallSkipReason, ToolExecutionOutcome};
use crate::claim::{AgentId, SessionId};
use crate::delegation::{DelegationId, DelegationStatus, DelegationSummary};
use crate::session::{
    HistoricalTimelineTurn, HistoricalTurn, TurnJournalStatus, TurnJournalTimelineItem,
    TurnJournalToolCall,
};
use crate::tool::ProcessSnapshot;

#[test]
fn start_separator_matches_terminal_width_without_trailing_blank() {
    let lines = super::inline_start_separator_lines_with_width(96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(lines
        .iter()
        .any(|line| line.contains("Agent Claim Network")));
    assert!(lines.iter().any(|line| line.contains("Runtime Metadata")));
    assert!(lines.iter().any(|line| line.contains("ACN 工作流")));
    assert!(lines.iter().any(|line| line.contains("Model ")));
    assert!(lines.iter().any(|line| line.contains("Agent ")));
    assert!(lines.iter().any(|line| line.contains("Cwd ")));
    assert!(lines.iter().any(|line| line.contains("Branch ")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Maintainer ❓  Router ❓")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Roles       Agent · Router · Maintainer")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Memory      偏好与经验沉淀 → 私有记忆")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Claim       可协作的判断对象 → 团队可见")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Router      团队信息检索器")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Maintainer  团队管理与台账")));
    assert!(lines.last().is_some_and(|line| !line.is_empty()));
    assert!(super::inline_start_separator_lines_with_width(96)
        .iter()
        .all(|line| line.width() <= 96));
}

#[test]
fn startup_welcome_keeps_exactly_one_blank_line_before_input() {
    let state = super::TuiState::new();
    let lines = super::inline_scrollback_lines_with_width(&state, 144)
        .into_iter()
        .chain(super::inline_live_lines_with_width(&state, 144))
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let border_index = lines
        .iter()
        .position(|line| line.starts_with('╰'))
        .expect("欢迎页底边应存在");
    let input_index = lines
        .iter()
        .position(|line| line.starts_with('›'))
        .expect("输入行应存在");

    assert_eq!(blank_lines_between(&lines, border_index, input_index), 1);
}

#[test]
fn first_submitted_user_keeps_one_blank_line_after_flushed_welcome() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();
    state.begin_pending_turn("hihi".into());

    let mut lines = vec!["╰──╯".to_string()];
    lines.extend(
        super::inline_scrollback_lines_with_width(&state, 144)
            .into_iter()
            .map(|line| line.to_string()),
    );
    let input_index = lines
        .iter()
        .position(|line| line.starts_with("› hihi"))
        .expect("首条已提交输入应进入 scrollback");

    assert_eq!(blank_lines_between(&lines, 0, input_index), 1);
}

#[test]
fn startup_welcome_uses_state_model_name() {
    let mut state = super::TuiState::new();
    state.model_name = Some("example-chat-model".into());

    let lines = super::inline_scrollback_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(lines
        .iter()
        .any(|line| line.contains("Model example-chat-model")));
    assert!(!lines.iter().any(|line| line.contains("Model not set")));
}

#[test]
fn startup_welcome_renders_latest_team_service_status() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::TeamServicesConnectionUpdated {
        status: TeamServicesConnectionStatus {
            maintainer: TeamServiceConnectionStatus::Connected,
            router: TeamServiceConnectionStatus::Failed,
        },
    });

    let lines = super::inline_scrollback_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(lines
        .iter()
        .any(|line| line.contains("Maintainer ✅  Router ❌")));
}

#[test]
fn background_completion_preserves_subagent_owner_and_signal_in_tui_event() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::BackgroundProcessCompleted {
        process_id: "deadbeef".into(),
        owner_agent_id: "agent-1".into(),
        owner_root_session_id: "session_1234abcd".into(),
        owner_subagent_id: Some("child-a".into()),
        status: "terminated".into(),
        exit_code: None,
        signal: Some(9),
    });

    let transcript = state.transcript_text();
    assert!(transcript.contains("owner=child-a"));
    assert!(transcript.contains("signal 9"));
    assert!(!transcript.contains("exit unknown"));
}

#[test]
fn startup_welcome_stays_before_first_turn() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });

    let initial_scrollback = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let initial_live = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(initial_scrollback[0].contains("Agent Claim Network"));
    assert!(initial_live.iter().any(|line| line.starts_with('›')));
    assert!(!initial_live
        .iter()
        .any(|line| line.contains("Agent Claim Network")));

    state.begin_pending_turn("第一条".into());
    let mut running = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    running.extend(
        super::inline_live_lines_with_width(&state, 80)
            .into_iter()
            .map(|line| line.to_string()),
    );
    let welcome_index = running
        .iter()
        .position(|line| line.contains("Agent Claim Network"))
        .unwrap();
    let prompt_index = running
        .iter()
        .position(|line| line.contains("第一条"))
        .unwrap();
    assert!(welcome_index < prompt_index);

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    let scrollback = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let welcome_index = scrollback
        .iter()
        .position(|line| line.contains("Agent Claim Network"))
        .unwrap();
    let prompt_index = scrollback
        .iter()
        .position(|line| line.contains("第一条"))
        .unwrap();
    assert!(welcome_index < prompt_index);

    state.mark_start_separator_flushed();
    let after_flush = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(!after_flush
        .iter()
        .any(|line| line.contains("Agent Claim Network")));
}

#[test]
fn startup_welcome_is_scrollback_prelude_while_syncing_inbox() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::StartupProgress {
        label: "processing inbox...".into(),
    });

    let scrollback_text = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let live_text = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(scrollback_text.contains("Agent Claim Network"));
    assert!(!live_text.contains("Agent Claim Network"));
    assert!(live_text.contains("processing inbox"));
}

#[test]
fn startup_welcome_reflows_for_hard_clear_before_history_exists() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();

    let before_reset = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!before_reset.contains("Agent Claim Network"));

    state.reset_flushed_for_hard_clear();

    let after_reset = super::inline_scrollback_lines_with_width(&state, 72)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert!(after_reset
        .iter()
        .any(|line| line.contains("Agent Claim Network")));
    assert!(after_reset.iter().all(|line| line.chars().count() <= 72));
}

#[test]
fn startup_welcome_reflushes_on_hard_clear_after_history_exists() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.mark_start_separator_flushed();
    state.begin_pending_turn("你好".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "你好，我在。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    state.reset_flushed_for_hard_clear();

    // hard_clear 会 Purge 终端 scrollback（含 welcome 横幅），故重排时必须无条件重发横幅，
    // 且横幅排在重排历史之前，保证 resize/reload 后 scrollback 顶部仍有欢迎卡片。
    let after_resize = super::inline_scrollback_lines_with_width(&state, 72)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let welcome_index = after_resize
        .iter()
        .position(|line| line.contains("Agent Claim Network"));
    let history_index = after_resize.iter().position(|line| line.contains("你好"));
    assert!(welcome_index.is_some(), "重排后应重发 welcome 横幅");
    assert!(welcome_index < history_index, "横幅应排在重排历史之前");
    let joined = after_resize.join("\n");
    assert!(joined.contains("你好"));
    assert!(joined.contains("你好，我在。"));
}

#[test]
fn startup_welcome_flushes_before_resumed_history() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.push_historical_turns(&[HistoricalTurn {
        user_text: "恢复第一条".into(),
        assistant_text: Some("恢复回复".into()),
    }]);

    let scrollback = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let welcome_index = scrollback
        .iter()
        .position(|line| line.contains("Agent Claim Network"))
        .unwrap();
    let resumed_index = scrollback
        .iter()
        .position(|line| line.contains("恢复第一条"))
        .unwrap();
    assert!(welcome_index < resumed_index);
    let border_index = scrollback
        .iter()
        .position(|line| line.starts_with('╰'))
        .expect("欢迎页底边应存在");
    assert_eq!(
        blank_lines_between(&scrollback, border_index, resumed_index),
        1
    );

    let live_text = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!live_text.contains("Agent Claim Network"));
}

#[test]
fn inline_live_region_keeps_status_below_composer() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.mark_start_separator_flushed();

    let lines = super::inline_live_lines_with_width(&state, 120)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(lines[0].is_empty());
    assert!(lines[1].starts_with('›'));
    assert!(lines
        .iter()
        .any(|line| line.contains("ready · claim network")));
    assert!(lines.iter().any(|line| line.contains("agent agent-a")));
    assert!(lines
        .iter()
        .any(|line| line.contains("Whisper your wish here...")));
    assert!(lines.iter().any(|line| line.contains("model")));
    assert!(lines.iter().any(|line| line.contains("ctx 0k/200k")));
    assert!(lines.iter().any(|line| line.contains("branch")));
}

#[test]
fn live_region_shows_local_claims_and_last_router_lookup() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.apply_event(SessionEvent::LocalClaimsUpdated { total: 7 });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_router".into(),
        summary: "tool consult_router ok claims=3 disputes=1".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.mark_start_separator_flushed();

    let live_text = super::inline_live_lines_with_width(&state, 88)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(live_text.contains("local claims 7"));
    assert!(live_text.contains("last router consult claims 3 · disputes 1"));
}

#[test]
fn live_region_tracks_recent_acn_contribution() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::InboxCompleted {
        processed: 2,
        new_claim_ids: vec!["claim_00000001".parse().unwrap()],
        updated_claim_ids: vec!["claim_00000002".parse().unwrap()],
        new_dispute_ids: vec!["dispute_00000001".parse().unwrap()],
        deprecated_claim_ids: vec![],
    });
    state.mark_start_separator_flushed();

    let inbox_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(inbox_text.contains("inbox 2 · claims +1 / ~1 / -0 · disputes +1"));

    state.apply_event(SessionEvent::FinalizeCompleted {
        trace_id: None,
        new_claim_ids: vec!["claim_00000003".parse().unwrap()],
        updated_claim_ids: vec!["claim_00000005".parse().unwrap()],
        new_dispute_ids: vec![],
    });

    let finalize_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(finalize_text.contains("finalize · claims +1 / ~1 / -0 · disputes +0"));

    state.apply_event(SessionEvent::CompactionCompleted {
        compacted_until: 8,
        recapped_until: 18,
        new_claim_ids: vec!["claim_00000004".parse().unwrap()],
        updated_claim_ids: vec!["claim_00000006".parse().unwrap()],
        new_dispute_ids: vec!["dispute_00000002".parse().unwrap()],
    });

    let compact_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(compact_text.contains("compact · claims +1 / ~1 / -0 · disputes +1"));
}

#[test]
fn normal_turn_clears_recent_acn_contribution_but_keeps_network_snapshot() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::LocalClaimsUpdated { total: 9 });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_router".into(),
        summary: "tool consult_router ok claims=16 disputes=0".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::InboxCompleted {
        processed: 1,
        new_claim_ids: vec!["claim_00000001".parse().unwrap()],
        updated_claim_ids: vec![],
        new_dispute_ids: vec![],
        deprecated_claim_ids: vec![],
    });
    state.mark_start_separator_flushed();

    state.begin_pending_turn("你是谁".into());

    let live_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(live_text.contains("local claims 9"));
    assert!(live_text.contains("last router consult claims 16 · disputes 0"));
    assert!(!live_text.contains("inbox 1 · claims +1 / ~0 / -0 · disputes +0"));
}

#[test]
fn delegation_status_line_renders_without_transcript_pollution() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![delegation_summary(
        "subagent_11111111",
        "scan files",
        "researcher",
        DelegationStatus::Running,
        Some("reading src"),
    )]);

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));

    assert!(live_text.contains("Subagents: 1 running · /subagents"));
    assert!(live_text.contains("/subagents"));
    assert!(!state.transcript_text().contains("subagent running"));
}

#[test]
fn delegation_status_line_distinguishes_queued_from_running() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![delegation_summary(
        "subagent_11111111",
        "queued work",
        "researcher",
        DelegationStatus::Queued,
        Some("waiting"),
    )]);

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));

    assert!(live_text.contains("Subagents: 1 queued · /subagents"));
    assert!(!live_text.contains("Subagents: 1 running"));
}

#[test]
fn delegation_status_line_summarizes_mixed_running_and_queued() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "running work",
            "researcher",
            DelegationStatus::Running,
            Some("reading"),
        ),
        delegation_summary(
            "subagent_22222222",
            "queued work",
            "researcher",
            DelegationStatus::Queued,
            Some("waiting"),
        ),
    ]);

    let live_text = lines_text(&super::inline_live_lines_with_size(&state, 96, 10));

    assert!(live_text.contains("Subagents: 1 running · 1 queued · /subagents"));
}

#[test]
fn delegation_status_line_stays_visible_after_all_subagents_complete() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "first done",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
        delegation_summary(
            "subagent_22222222",
            "second done",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
    ]);

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));

    assert!(live_text.contains("Subagents: 2 completed · /subagents"));
}

#[test]
fn delegation_status_line_summarizes_mixed_terminal_and_active_subagents() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "done",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
        delegation_summary(
            "subagent_22222222",
            "failed",
            "researcher",
            DelegationStatus::Failed,
            Some("failed"),
        ),
        delegation_summary(
            "subagent_33333333",
            "running",
            "researcher",
            DelegationStatus::Running,
            Some("reading"),
        ),
        delegation_summary(
            "subagent_44444444",
            "queued",
            "researcher",
            DelegationStatus::Queued,
            Some("waiting"),
        ),
    ]);

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));

    assert!(
        live_text.contains("Subagents: 1 completed · 1 failed · 1 running · 1 queued · /subagents")
    );
}

#[test]
fn background_status_orders_processes_before_inlined_subagent_notice() {
    let mut state = super::TuiState::new();
    state.set_process_snapshots(vec![
        process_snapshot("11111111", "running"),
        process_snapshot("22222222", "running"),
        process_snapshot("33333333", "terminating"),
    ]);
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "finished task",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
        delegation_summary(
            "subagent_22222222",
            "sleep5",
            "researcher",
            DelegationStatus::Failed,
            Some("failed"),
        ),
    ]);
    state.set_status_notice("Subagent 'sleep5' failed");

    let lines = state
        .background_status_lines(160)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        lines,
        vec![
            "Processes: 2 running · 1 terminating · /ps",
            "Subagents: 1 completed · 1 failed · Subagent 'sleep5' failed · /subagents",
        ]
    );
    assert!(state.status_notice_line().is_none());
}

#[test]
fn background_status_uses_compact_semantic_lines_when_the_full_summary_would_wrap() {
    let mut state = super::TuiState::new();
    state.set_process_snapshots(vec![
        process_snapshot("11111111", "running"),
        process_snapshot("22222222", "running"),
        process_snapshot("33333333", "terminating"),
    ]);
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "finished task",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
        delegation_summary(
            "subagent_22222222",
            "sleep5",
            "researcher",
            DelegationStatus::Failed,
            Some("failed"),
        ),
    ]);
    state.set_status_notice("Subagent 'sleep5' failed");

    let lines = state
        .background_status_lines(48)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(
        lines,
        vec![
            "Processes: 2 run · 1 stopping · /ps",
            "Subagents: 1 done · 1 failed · /subagents",
            "↳ Subagent 'sleep5' failed",
        ]
    );
    assert!(lines.iter().all(|line| line.width() <= 48));
    assert!(state.status_notice_line().is_none());

    let rendered = super::inline_live_lines_with_width(&state, 48)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert!(rendered
        .iter()
        .all(|line| UnicodeWidthStr::width(line.as_str()) <= 47));
    let process_index = rendered
        .iter()
        .position(|line| line == "Processes: 2 run · 1 stopping · /ps")
        .unwrap();
    let subagent_index = rendered
        .iter()
        .position(|line| line == "Subagents: 1 done · 1 failed · /subagents")
        .unwrap();
    assert!(process_index < subagent_index);
}

#[test]
fn delegation_panel_renders_read_only_snapshot() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "scan\nfiles",
            "researcher",
            DelegationStatus::Running,
            Some("reading\nsrc"),
        ),
        delegation_summary(
            "subagent_22222222",
            "verify tests",
            "verifier",
            DelegationStatus::Completed,
            None,
        ),
    ]);
    state.open_delegation_panel();

    let live_lines = super::inline_live_lines_with_size(&state, 96, 12);
    let plain = lines_plain_text(&live_lines);
    let live_text = plain.join("\n");

    assert_eq!(plain.len(), 12);
    assert!(live_text.contains("Session Subagents"));
    assert!(live_text.contains("Hash"));
    assert!(live_text.contains("Status"));
    assert!(live_text.contains("Title"));
    assert!(live_text.contains("Role"));
    assert!(live_text.contains("Update_time"));
    assert!(live_text.contains("Latest"));
    assert!(live_text.contains("read-only"));
    assert!(live_text.contains("↑/↓ to navigate  · Esc to back"));
    assert!(live_text.contains("model"));
    assert!(live_text.contains("running"));
    assert!(live_text.contains("11111111"));
    assert!(!live_text.contains("subagent_11111111"));
    assert!(live_text.contains("20"));
    assert!(live_text.contains("scan files"));
    assert!(live_text.contains("reading src"));
    assert!(live_text.contains("completed"));
    assert!(!live_text.contains("cancel"));
}

#[test]
fn delegation_panel_empty_state_replaces_table_header_with_blank_line() {
    let mut state = super::TuiState::new();
    state.open_delegation_panel();

    let lines = state.delegation_panel_lines(96, 5).unwrap();
    let plain = lines_plain_text(&lines);

    assert_eq!(plain[0], "Session Subagents  read-only");
    assert!(plain[1].is_empty());
    assert_eq!(plain[2], "No subagents in this session.");
    assert!(!plain.join("\n").contains("Hash      Status"));
}

#[test]
fn delegation_panel_colors_status_values() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "scan files",
            "researcher",
            DelegationStatus::Running,
            Some("reading src"),
        ),
        delegation_summary(
            "subagent_22222222",
            "verify tests",
            "verifier",
            DelegationStatus::Completed,
            None,
        ),
    ]);
    state.open_delegation_panel();

    let live_lines = super::inline_live_lines_with_size(&state, 120, 12);
    let running_span = live_lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.as_ref() == "running  ")
        .expect("Running status span");
    let completed_span = live_lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.as_ref() == "completed")
        .expect("Completed status span");

    assert_eq!(running_span.style.fg, Some(Color::Yellow));
    assert!(running_span.style.add_modifier.contains(Modifier::BOLD));
    assert_eq!(completed_span.style.fg, Some(Color::Green));
    assert!(completed_span.style.add_modifier.contains(Modifier::BOLD));
}

#[test]
fn delegation_panel_arrow_keys_scroll_fullscreen_body() {
    let (sender, mut rx) = super::app_event::AppEventSender::channel();
    let mut chat = super::chat_widget::ChatWidget::new(sender);
    chat.state_mut().set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "first subagent",
            "researcher",
            DelegationStatus::Running,
            Some("first visible step"),
        ),
        delegation_summary(
            "subagent_22222222",
            "second subagent",
            "verifier",
            DelegationStatus::Running,
            Some("second visible step"),
        ),
        delegation_summary(
            "subagent_33333333",
            "third subagent",
            "writer",
            DelegationStatus::Completed,
            Some("third visible step"),
        ),
    ]);
    chat.state_mut().open_delegation_panel();

    let before = lines_text(&super::inline_live_lines_with_size(chat.state(), 120, 7));
    assert!(before.contains("first subagent"));
    assert!(!before.contains("third subagent"));

    for _ in 0..4 {
        chat.handle_key_event(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(
            rx.try_recv().unwrap(),
            super::app_event::AppEvent::RenderRequested
        );
    }
    let after = lines_text(&super::inline_live_lines_with_size(chat.state(), 120, 7));
    assert!(!after.contains("first subagent"));
    assert!(after.contains("third subagent"));
}

#[test]
fn delegation_panel_opening_closes_mcp_panel() {
    let mut state = super::TuiState::new();

    state.open_mcp_panel();
    assert!(state.mcp_panel_visible());

    state.open_delegation_panel();

    assert!(state.delegation_panel_visible());
    assert!(!state.mcp_panel_visible());
}

#[test]
fn delegation_panel_renders_snapshot_error_without_stale_active_count() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![delegation_summary(
        "subagent_11111111",
        "scan files",
        "researcher",
        DelegationStatus::Running,
        Some("reading src"),
    )]);
    state.set_delegation_snapshot_error("Read failed");

    let status_text = lines_text(&super::inline_live_lines_with_width(&state, 96));
    assert!(status_text.contains("Subagents: status unavailable · /subagents"));

    state.open_delegation_panel();

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 96));

    assert!(live_text.contains("Subagent snapshot unavailable: Read failed"));
    assert!(!live_text.contains("Subagents: 1 running"));
    assert!(!live_text.contains("scan files"));
}

#[test]
fn delegation_panel_prioritizes_terminal_latest_over_stale_step() {
    let mut state = super::TuiState::new();
    let mut summary = delegation_summary(
        "subagent_33333333",
        "finish report",
        "writer",
        DelegationStatus::Completed,
        Some("still reading old step"),
    );
    summary.result_ref = Some("result.md".into());
    summary.progress_summary = Some("final summary".into());
    summary.changed_files = vec!["src/lib.rs".into()];
    state.set_delegation_summaries(vec![summary]);
    state.open_delegation_panel();

    let live_text = lines_text(&super::inline_live_lines_with_size(&state, 160, 10));

    assert!(live_text.contains("Completed: final summary"));
    assert!(live_text.contains("changed: src/lib.rs"));
    assert!(live_text.contains("latest: final summary"));
    assert!(!live_text.contains("result: result.md"));
    assert!(!live_text.contains("still reading old step"));
}

#[test]
fn delegation_panel_prioritizes_progress_over_step_for_active_subagents() {
    let mut state = super::TuiState::new();
    let mut summary = delegation_summary(
        "subagent_66666666",
        "scan files",
        "researcher",
        DelegationStatus::Running,
        Some("reading-src"),
    );
    summary.progress_summary = Some("Reading source files and checking invariants".into());
    state.set_delegation_summaries(vec![summary]);
    state.open_delegation_panel();

    let live_text = lines_text(&super::inline_live_lines_with_size(&state, 160, 10));

    assert!(live_text.contains("Reading source files and checking invariants"));
    assert!(live_text.contains("step: reading-src"));
    assert!(live_text.contains("latest: Reading source files and checking invariants"));
}

#[test]
fn delegation_panel_keeps_changed_files_visible_with_long_result_ref() {
    let mut state = super::TuiState::new();
    let mut summary = delegation_summary(
        "subagent_44444444",
        "finish report",
        "writer",
        DelegationStatus::Completed,
        Some("old step"),
    );
    summary.result_ref = Some(format!("result-{}.md", "x".repeat(200)));
    summary.changed_files = vec!["src/lib.rs".into()];
    state.set_delegation_summaries(vec![summary]);
    state.open_delegation_panel();

    let live_text = lines_text(&super::inline_live_lines_with_size(&state, 96, 10));

    assert!(live_text.contains("changed: src/lib.rs"));
}

#[test]
fn delegation_panel_marks_omitted_changed_files() {
    let mut state = super::TuiState::new();
    let mut summary = delegation_summary(
        "subagent_55555555",
        "touch files",
        "writer",
        DelegationStatus::Completed,
        None,
    );
    summary.result_ref = Some("result.md".into());
    summary.changed_files = vec![
        "src/a.rs".into(),
        "src/b.rs".into(),
        "src/c.rs".into(),
        "src/d.rs".into(),
        "src/e.rs".into(),
    ];
    state.set_delegation_summaries(vec![summary]);
    state.open_delegation_panel();

    let live_text = lines_text(&super::inline_live_lines_with_size(&state, 160, 10));

    assert!(live_text.contains("changed(+2 more): src/a.rs, src/b.rs, src/c.rs"));
    assert!(live_text.contains("Completed"));
    assert!(!live_text.contains("result: result.md"));
}

/// 首个 L 块落定后的棋盘字符画指纹（由 span_char_art 从样式反解码而来：
/// 满块实际由背景色绘制，见 turn_animation::cell_span 对行距 >1 露缝的修复）。
const FIRST_TURN_RESIDUE_BOARD: &str = "    \n▄   \n█▄  ";

fn lines_plain_text(lines: &[Line<'static>]) -> Vec<String> {
    lines.iter().map(ToString::to_string).collect()
}

fn lines_text(lines: &[Line<'static>]) -> String {
    lines_plain_text(lines).join("\n")
}

fn blank_lines_between(lines: &[String], first: usize, second: usize) -> usize {
    lines[first.saturating_add(1)..second]
        .iter()
        .filter(|line| line.trim().is_empty())
        .count()
}

fn delegation_summary(
    id: &str,
    title: &str,
    role: &str,
    status: DelegationStatus,
    current_step: Option<&str>,
) -> DelegationSummary {
    let now = Utc::now();
    DelegationSummary {
        id: DelegationId::from_str(id).unwrap(),
        title: title.into(),
        role: role.into(),
        status,
        current_step: current_step.map(str::to_string),
        created_at: now,
        updated_at: now,
        started_at: None,
        completed_at: None,
        error_summary: None,
        progress_summary: None,
        result_ref: None,
        changed_files: Vec::new(),
    }
}

fn process_snapshot(process_id: &str, status: &str) -> ProcessSnapshot {
    ProcessSnapshot {
        process_id: process_id.into(),
        instance_id: 1,
        root_session_id: "session_1234abcd".into(),
        subagent_id: None,
        status: status.into(),
        tty: false,
        command: "sleep 600".into(),
        code_type: "bash".into(),
        cwd: "/workspace".into(),
        started_at: SystemTime::now(),
    }
}

/// 按样式反解码的棋盘字符画：背景色满块还原成 '█'，比纯文本 to_string 更能反映形状。
fn micro_tetris_board_text(state: &super::TuiState, width: u16) -> String {
    state
        .running_turn_animation_lines(width, 3)
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(super::turn_animation::span_char_art)
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// 棋盘 cell 的颜色信息在 span 样式上（背景色满块），必须按样式而非纯文本判断。
fn micro_tetris_line_has_cell(line: &Line<'static>) -> bool {
    line.spans
        .iter()
        .any(super::turn_animation::span_paints_cell)
}

fn micro_tetris_has_cell(lines: &[Line<'static>]) -> bool {
    lines.iter().any(micro_tetris_line_has_cell)
}

fn micro_tetris_full_text(state: &super::TuiState, width: u16) -> String {
    super::inline_scrollback_lines_with_width(state, width)
        .into_iter()
        .chain(super::inline_live_lines_with_width(state, width))
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn live_box_content_line_count(lines: &[String], title: &str) -> usize {
    let top_index = lines
        .iter()
        .position(|line| line.contains(title))
        .expect("Live box title should be visible");
    let bottom_index = lines[top_index..]
        .iter()
        .position(|line| line.trim_start().starts_with('└'))
        .map(|offset| top_index + offset)
        .expect("Live box bottom border should be visible");
    bottom_index.saturating_sub(top_index).saturating_sub(1)
}

fn finish_pending_micro_tetris_commit(state: &mut super::TuiState) {
    for _ in 0..16 {
        let _ = state.tick_turn_animation(80, 3);
    }
    assert!(!state.turn_animation_is_active());
}

#[test]
fn running_turn_displays_micro_tetris_board() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("帮我优化这个模块".into());

    let live_lines = super::inline_live_lines_with_width(&state, 80);
    let live_strings = lines_plain_text(&live_lines);
    let live_text = live_strings.join("\n");
    let title_index = live_strings
        .iter()
        .position(|line| line.contains("Working · Streaming response"))
        .expect("Running live box title should be visible");
    let content_line_after_title = live_lines
        .get(title_index.saturating_add(1))
        .expect("Running live box content line should be visible");

    assert!(live_text.contains("Working · Streaming response"));
    assert!(micro_tetris_has_cell(&live_lines));
    assert!(micro_tetris_line_has_cell(content_line_after_title));
    assert_eq!(
        live_box_content_line_count(&live_strings, "Working · Streaming response"),
        3
    );
    assert!(!live_text.contains("╭╌"));
}

#[test]
fn live_box_content_uses_dashed_vertical_borders() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("检查边框样式".into());

    let live_lines = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let title_line = live_lines
        .iter()
        .find(|line| line.contains("Working · Streaming response"))
        .expect("Running live box title should be visible");

    assert!(title_line.starts_with('┌'));
    assert!(title_line.ends_with('┐'));
    let content_line = live_lines
        .iter()
        .find(|line| line.starts_with('┆'))
        .expect("Live box content line should use dashed vertical border");
    assert!(content_line.ends_with('┆'));
    assert!(!content_line.starts_with('│'));
    assert!(!content_line.ends_with('│'));
}

#[test]
fn slash_command_states_do_not_display_micro_tetris_board() {
    let mut state = super::TuiState::new();
    state.push_help();

    let help_lines = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .chain(super::inline_live_lines_with_width(&state, 80))
        .collect::<Vec<_>>();
    assert!(!micro_tetris_has_cell(&help_lines));

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::SyncingInbox,
    });
    state.apply_event(SessionEvent::InboxStarted);
    let inbox_lines = super::inline_live_lines_with_width(&state, 80);
    assert!(lines_text(&inbox_lines).contains("Inbox · Syncing updates"));
    assert!(!micro_tetris_has_cell(&inbox_lines));

    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Finalizing,
    });
    state.apply_event(SessionEvent::FinalizeStarted);
    let finalize_lines = super::inline_live_lines_with_width(&state, 80);
    let finalize_text = lines_text(&finalize_lines);
    assert!(finalize_text.contains("Finalizing · Committing contribution"));
    assert!(finalize_text.contains("finalizing session_1234abcd..."));
    assert!(!micro_tetris_has_cell(&finalize_lines));
}

#[test]
fn narrow_width_hides_micro_tetris_and_does_not_advance_tick() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("窄屏不要显示动画".into());
    let before = state.running_turn_animation_lines(80, 3);

    assert!(!state.tick_turn_animation(47, 3));

    let after = state.running_turn_animation_lines(80, 3);
    let narrow_lines = super::inline_live_lines_with_width(&state, 47);

    assert_eq!(before, after);
    assert!(!micro_tetris_has_cell(&narrow_lines));
}

#[test]
fn short_height_hides_micro_tetris_and_does_not_advance_tick() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("矮屏不要显示动画".into());
    let before = micro_tetris_board_text(&state, 80);

    assert!(!state.tick_turn_animation(80, 2));

    let after = micro_tetris_board_text(&state, 80);
    let short_lines = super::inline_live_lines_with_size(&state, 80, 6);

    assert_eq!(before, after);
    assert!(!micro_tetris_has_cell(&short_lines));
}

#[test]
fn composer_spacer_counts_against_micro_tetris_height_budget() {
    let state = super::TuiState::new();

    assert_eq!(
        super::turn_animation_height_budget_for_test(&state, 80, 8),
        0
    );
    assert!(super::turn_animation_height_budget_for_test(&state, 80, 9) > 0);
}

#[test]
fn micro_tetris_width_threshold_uses_terminal_width() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("刚好超过阈值时要显示".into());

    let hidden_lines = super::inline_live_lines_with_width(&state, 47);
    let live_lines = super::inline_live_lines_with_width(&state, 48);

    assert!(!micro_tetris_has_cell(&hidden_lines));
    assert!(micro_tetris_has_cell(&live_lines));
}

#[test]
fn slash_command_status_hides_and_settles_pending_final_animation() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });
    assert!(state.turn_animation_is_active());

    state.settle_turn_animation_before_command();
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::SyncingInbox,
    });
    state.apply_event(SessionEvent::InboxStarted);

    let inbox_lines = super::inline_live_lines_with_width(&state, 80);
    assert!(!state.turn_animation_is_active());
    assert!(!state.tick_turn_animation(80, 3));
    assert!(!micro_tetris_has_cell(&inbox_lines));

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });
    let settled_after_command = micro_tetris_board_text(&state, 80);
    assert_eq!(settled_after_command, FIRST_TURN_RESIDUE_BOARD);
    state.begin_pending_turn("第二条".into());
    let next_turn_animation = micro_tetris_board_text(&state, 80);
    assert!(!next_turn_animation.is_empty());
    assert_ne!(next_turn_animation, settled_after_command);
}

#[test]
fn help_command_settles_pending_final_micro_tetris() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });
    assert!(!state.running_turn_animation_lines(80, 3).is_empty());

    state.settle_turn_animation_before_command();
    state.push_help();

    let help_lines = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .chain(super::inline_live_lines_with_width(&state, 80))
        .collect::<Vec<_>>();

    assert!(!state.turn_animation_is_active());
    assert!(!state.tick_turn_animation(80, 3));
    assert!(lines_text(&help_lines).contains("ACN commands"));
    assert!(micro_tetris_has_cell(&help_lines));
    assert_eq!(
        micro_tetris_board_text(&state, 80),
        FIRST_TURN_RESIDUE_BOARD
    );
}

#[test]
fn committed_turn_finishes_micro_tetris_then_idle_keeps_residue_visible() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    let finalizing_lines = super::inline_live_lines_with_width(&state, 80);
    assert!(micro_tetris_has_cell(&finalizing_lines));
    assert!(lines_text(&finalizing_lines).contains("Idle"));

    finish_pending_micro_tetris_commit(&mut state);

    let open_lines = super::inline_live_lines_with_width(&state, 80);
    let open_strings = lines_plain_text(&open_lines);
    let open_text = open_strings.join("\n");
    assert!(open_text.contains("Idle"));
    assert!(micro_tetris_has_cell(&open_lines));
    assert_eq!(live_box_content_line_count(&open_strings, "Idle"), 3);
    assert_eq!(
        micro_tetris_board_text(&state, 80),
        FIRST_TURN_RESIDUE_BOARD
    );
    assert!(!open_text.contains("Working · Streaming response"));
}

#[test]
fn cancel_and_fail_show_last_committed_micro_tetris_residue() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });
    finish_pending_micro_tetris_commit(&mut state);
    let committed_board = micro_tetris_board_text(&state, 80);

    assert_eq!(committed_board, FIRST_TURN_RESIDUE_BOARD);

    state.begin_pending_turn("第二条".into());
    assert!(state.turn_animation_is_active());
    state.cancel_running_turn("user pressed esc");

    let cancelled_board = micro_tetris_board_text(&state, 80);
    assert!(!state.turn_animation_is_active());
    assert_eq!(cancelled_board, committed_board);
    assert!(micro_tetris_full_text(&state, 80).contains("Turn cancelled"));

    state.begin_pending_turn("第三条".into());
    assert!(state.turn_animation_is_active());
    state.fail_running_turn("provider timeout");

    let failed_board = micro_tetris_board_text(&state, 80);
    assert!(!state.turn_animation_is_active());
    assert_eq!(failed_board, committed_board);
    assert!(micro_tetris_full_text(&state, 80).contains("Turn failed"));
}

#[test]
fn help_command_preserves_static_committed_micro_tetris() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });
    finish_pending_micro_tetris_commit(&mut state);

    assert!(!micro_tetris_board_text(&state, 80).is_empty());

    state.settle_turn_animation_before_command();
    state.push_help();

    let help_lines = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .chain(super::inline_live_lines_with_width(&state, 80))
        .collect::<Vec<_>>();

    assert!(!state.turn_animation_is_active());
    assert!(!state.tick_turn_animation(80, 3));
    assert!(lines_text(&help_lines).contains("ACN commands"));
    assert!(micro_tetris_has_cell(&help_lines));
    assert_eq!(
        micro_tetris_board_text(&state, 80),
        FIRST_TURN_RESIDUE_BOARD
    );
}

#[test]
fn finalize_failure_after_settle_preserves_static_micro_tetris_residue() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    state.settle_turn_animation_before_command();
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Finalizing,
    });
    state.apply_event(SessionEvent::FinalizeFailed {
        error: "provider timeout".into(),
    });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Error,
    });

    let error_lines = super::inline_live_lines_with_width(&state, 80);
    assert!(!state.turn_animation_is_active());
    assert!(!state.tick_turn_animation(80, 3));
    assert!(state.finalize_failed());
    assert!(!state.input_accepts_text());
    assert_eq!(
        super::composer_hint(&state),
        "Finalize failed · Ctrl+C quit"
    );
    assert!(lines_text(&error_lines).contains("Attention · Last turn failed"));
    assert!(lines_text(&error_lines).contains("Finalize failed · Ctrl+C quit"));
    assert!(!lines_text(&error_lines).contains("Whisper your wish here..."));
    assert!(micro_tetris_has_cell(&error_lines));
    assert_eq!(
        micro_tetris_board_text(&state, 80),
        FIRST_TURN_RESIDUE_BOARD
    );
}

#[test]
fn hidden_final_micro_tetris_does_not_force_working_title_on_narrow_width() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::LocalClaimsUpdated { total: 1 });
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    let narrow_lines = super::inline_live_lines_with_width(&state, 47);

    assert!(state.turn_animation_is_active());
    assert!(!micro_tetris_has_cell(&narrow_lines));
    assert!(!lines_text(&narrow_lines).contains("Working · Streaming response"));
}

#[test]
fn resumed_session_does_not_show_idle_micro_tetris() {
    let mut state = super::TuiState::new();
    state.set_status_notice("Subagent old completed");
    state.reset_for_resumed_session();
    state.status = SessionRuntimeStatus::Open;
    state.push_historical_turns(&[HistoricalTurn {
        user_text: "旧问题".into(),
        assistant_text: Some("旧回复".into()),
    }]);
    let resumed_lines = super::inline_live_lines_with_width(&state, 80);

    assert!(!state.turn_animation_is_active());
    assert!(!micro_tetris_has_cell(&resumed_lines));
    assert!(!lines_text(&resumed_lines).contains("Subagent old completed"));
}

#[test]
fn narrow_live_region_uses_compact_footer_text() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.mark_start_separator_flushed();

    let lines = super::inline_live_lines_with_width(&state, 44)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(lines.iter().any(|line| line == "type /"));
    assert!(lines.iter().any(|line| line == "open"));
    assert!(!lines.iter().any(|line| line.contains("Shift+Enter")));
    assert!(!lines.iter().any(|line| line.contains("tok --")));
}

#[test]
fn initialization_progress_is_visible_and_keeps_commands_available() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::StartupProgress {
        label: "processing inbox...".into(),
    });

    assert_eq!(state.status_label(), "initializing");
    assert!(super::input_accepts_text(state.status));
    assert!(super::history_render_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("processing inbox")));
    assert_eq!(super::composer_hint(&state), "initializing session...");
}

#[test]
fn open_hint_mentions_compact_command() {
    let mut state = super::TuiState::new();
    state.push_input_text("/");

    let text = super::composer_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("/compact"));
}

#[test]
fn open_footer_prefixes_bold_session_id_when_available() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });

    let lines = super::composer_lines_with_width(&state, 80);
    let footer = lines.last().expect("Composer footer should render");

    assert_eq!(
        footer.to_string(),
        "session_1234abcd type / for commands · Enter sends"
    );
    assert_eq!(footer.spans[0].content.as_ref(), "session_1234abcd");
    assert!(footer.spans[0].style.add_modifier.contains(Modifier::BOLD));
}

#[test]
fn running_footer_explains_queue_steer_and_cancel_keys() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("active".into());

    assert_eq!(
        super::composer_hint(&state),
        "Enter queues · Ctrl+Enter steers · Esc recalls queue/cancels · Ctrl+C cancels"
    );
}

#[test]
fn running_footer_uses_shorter_esc_hint_on_narrow_width() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("active".into());

    let lines = super::composer_lines_with_width(&state, 48);
    let footer = lines.last().expect("Footer should render");

    assert!(footer.to_string().contains("Esc recalls"));
    assert!(footer.width() <= 48);
}

#[test]
fn slash_menu_lists_matching_commands_and_bolds_first_match() {
    let mut state = super::TuiState::new();
    state.push_input_text("/");

    let lines = super::composer_lines_with_width(&state, 96);
    let text = lines
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    // 9 条原生命令超过 5 行窗口：只显示前 5 条（字母序），其余靠上下键滚动
    assert!(text.contains("/compact"));
    assert!(text.contains("/copy"));
    assert!(text.contains("/inbox"));
    assert!(!text.contains("/skills"));
    assert!(text.find("/compact").unwrap() < text.find("/copy").unwrap());
    assert!(text.find("/copy").unwrap() < text.find("/exit").unwrap());
    let compact_span = lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.starts_with("/compact"))
        .expect("First slash command should render");
    assert!(compact_span.style.add_modifier.contains(Modifier::BOLD));
}

#[test]
fn slash_menu_puts_skills_first_and_scrolls_window_with_selection() {
    let mut state = super::TuiState::new();
    state.set_slash_skills([
        ("verify", "运行完整验证"),
        ("tui-smoke-test-with-tmux", "tmux 冒烟测试"),
    ]);
    state.push_input_text("/");

    let text = super::composer_lines_with_width(&state, 96)
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    // skills 字母序在前，原生命令随后；5 行窗口只露出前 3 条原生命令
    let tmux = text
        .find("/tui-smoke-test-with-tmux")
        .expect("Skill 应显示");
    let verify = text.find("/verify").expect("Skill 应显示");
    let compact = text.find("/compact").expect("原生命令应显示");
    assert!(tmux < verify && verify < compact);
    assert!(!text.contains("/inbox"));

    // 连续向下移动选中，窗口跟随滚动，末尾的原生命令进入视野
    for _ in 0..10 {
        assert!(state.select_next_slash_completion());
    }
    let text = super::composer_lines_with_width(&state, 96)
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("/skills"));
    assert!(!text.contains("/tui-smoke-test-with-tmux"));

    // 回车/Tab 补全取选中项（此时选中最后一项 /skills）
    assert!(state.accept_slash_completion());
    assert_eq!(state.input(), "/skills");
}

#[test]
fn option_arrow_keys_jump_words_in_composer() {
    let (sender, _rx) = super::app_event::AppEventSender::channel();
    let mut chat = super::chat_widget::ChatWidget::new(sender);
    for ch in "hiahi/home 今天几月几what words".chars() {
        chat.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }

    // Alt+← 两次：words 词首 → CJK+latin 连续段词首；插入定位符验证光标落点
    chat.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
    assert_eq!(chat.state().input(), "hiahi/home X今天几月几what words");

    // Alt+→（ESC f 风格同样生效）：跳到下一词首
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('f'), KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::NONE));
    assert_eq!(chat.state().input(), "hiahi/home X今天几月几what Ywords");

    // ESC b（Terminal.app 的 Option+←）：回到本词词首；'/' 是词边界
    let (sender, _rx) = super::app_event::AppEventSender::channel();
    let mut chat = super::chat_widget::ChatWidget::new(sender);
    for ch in "hiahi/home".chars() {
        chat.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('b'), KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('Z'), KeyModifiers::NONE));
    assert_eq!(chat.state().input(), "hiahi/Zhome");
    // Ctrl+← 不再等价于 Alt+←：退化为单字符左移，词跳转只认 Option
    chat.handle_key_event(KeyEvent::new(KeyCode::End, KeyModifiers::NONE));
    chat.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::CONTROL));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('W'), KeyModifiers::NONE));
    assert_eq!(chat.state().input(), "hiahi/ZhomWe");
}

#[test]
fn option_word_jump_requires_option_as_the_only_modifier() {
    fn chat_with_input(input: &str) -> super::chat_widget::ChatWidget {
        let (sender, _rx) = super::app_event::AppEventSender::channel();
        let mut chat = super::chat_widget::ChatWidget::new(sender);
        chat.state_mut().push_input_text(input);
        chat
    }

    for modifiers in [
        KeyModifiers::ALT | KeyModifiers::CONTROL,
        KeyModifiers::ALT | KeyModifiers::SHIFT,
    ] {
        // 只有 Option 本身可触发词跳转；叠加 Ctrl 或 Shift 时保留普通方向键行为。
        let mut chat = chat_with_input("one two");
        chat.handle_key_event(KeyEvent::new(KeyCode::Left, modifiers));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "one twXo");

        let mut chat = chat_with_input("one two");
        chat.handle_key_event(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Right, modifiers));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "oXne two");

        let mut chat = chat_with_input("one two");
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('b'), modifiers));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "one twoX");

        let mut chat = chat_with_input("one two");
        chat.handle_key_event(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('f'), modifiers));
        chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
        assert_eq!(chat.state().input(), "Xone two");
    }
}

#[test]
fn option_arrow_keys_skip_paste_and_image_placeholders() {
    let (sender, _rx) = super::app_event::AppEventSender::channel();
    let mut chat = super::chat_widget::ChatWidget::new(sender);
    for ch in "before [Pasted Content 1200 chars] after [Image #2] end".chars() {
        chat.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }

    chat.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Left, KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE));
    assert_eq!(
        chat.state().input(),
        "before [Pasted Content 1200 chars] Xafter [Image #2] end"
    );

    chat.handle_key_event(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
    chat.handle_key_event(KeyEvent::new(KeyCode::Right, KeyModifiers::ALT));
    chat.handle_key_event(KeyEvent::new(KeyCode::Char('Y'), KeyModifiers::NONE));
    assert_eq!(
        chat.state().input(),
        "before [Pasted Content 1200 chars] YXafter [Image #2] end"
    );
}

#[test]
fn tab_accepts_inline_slash_hint_mid_input() {
    let (sender, _rx) = super::app_event::AppEventSender::channel();
    let mut chat = super::chat_widget::ChatWidget::new(sender);
    chat.state_mut()
        .set_slash_skills([("tui-smoke-test-with-tmux", "tmux 冒烟测试")]);
    for ch in "你是谁 /tui-sm".chars() {
        chat.handle_key_event(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
    }

    chat.handle_key_event(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    assert_eq!(chat.state().input(), "你是谁 /tui-smoke-test-with-tmux");
}

#[test]
fn slash_menu_completes_skill_by_prefix_and_tab() {
    let mut state = super::TuiState::new();
    state.set_slash_skills([
        ("verify", "运行完整验证"),
        ("tui-smoke-test-with-tmux", "tmux 冒烟测试"),
    ]);
    for ch in "/tui-".chars() {
        state.push_input_char(ch);
    }
    assert!(state.slash_menu_visible());
    assert!(state.accept_slash_completion());
    assert_eq!(state.input(), "/tui-smoke-test-with-tmux");
    // 补全为精确命令后菜单收起
    assert!(!state.slash_menu_visible());
}

#[test]
fn status_footer_prefers_model_cwd_branch_ctx_and_focus() {
    let mut state = super::TuiState::new();
    state.model_name = Some("example-model".into());
    state.set_focus_duration_for_test(Duration::from_secs(95));

    let text = super::inline_live_lines_with_width(&state, 120)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(text.contains("model example-model"));
    assert!(text.contains("cwd "));
    assert!(text.contains("branch "));
    assert!(text.contains("ctx 0k/200k"));
    assert!(text.contains("focus 1m"));
    assert!(!text.contains("cache --"));
}

#[test]
fn status_footer_updates_context_usage_and_freezes_while_compacting() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::ContextUsageUpdated {
        used_tokens: 135_600,
    });
    let text = super::inline_live_lines_with_width(&state, 120)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("ctx 136k/200k"));

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Compacting,
    });
    state.apply_event(SessionEvent::ContextUsageUpdated {
        used_tokens: 42_000,
    });
    let compacting_text = super::inline_live_lines_with_width(&state, 120)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(compacting_text.contains("ctx 136k/200k"));
}

#[test]
fn help_mentions_compact_command() {
    let mut state = super::TuiState::new();

    state.push_help();

    assert!(state.transcript_text().contains("/compact"));
}

#[test]
fn help_aligns_and_bolds_command_column() {
    let mut state = super::TuiState::new();

    state.push_help();

    let help_lines = super::inline_scrollback_lines_with_width(&state, 120);
    let copy_line = help_lines
        .iter()
        .find(|line| line.to_string().contains("/copy"))
        .expect("/copy help row should render");

    assert_eq!(
        copy_line.to_string(),
        "  /copy       copy the last Assistant response"
    );
    assert_eq!(copy_line.spans[0].content.as_ref(), "  ");
    assert_eq!(copy_line.spans[1].content.as_ref(), "/copy       ");
    assert!(copy_line.spans[1]
        .style
        .add_modifier
        .contains(Modifier::BOLD));
    assert!(!copy_line.spans[2]
        .style
        .add_modifier
        .contains(Modifier::BOLD));
}

#[test]
fn help_wraps_long_descriptions_under_description_column() {
    let mut state = super::TuiState::new();

    state.push_help();

    let lines = lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 48));
    let ctrl_o_index = lines
        .iter()
        .position(|line| line.contains("Ctrl+O"))
        .expect("Ctrl+O help row should render");
    let continuation = lines
        .get(ctrl_o_index + 1)
        .expect("Ctrl+O help row should wrap at width 48");

    assert!(lines.iter().all(|line| line.width() <= 48));
    assert!(continuation.starts_with("              "));
    assert!(!continuation.contains("Ctrl+O"));
}

#[test]
fn help_lists_slash_commands_alphabetically() {
    let mut state = super::TuiState::new();

    state.push_help();

    let text = state.transcript_text();
    let compact = text.find("/compact").expect("/compact should render");
    let copy = text.find("/copy").expect("/copy should render");
    let exit = text.find("/exit").expect("/exit should render");
    let help = text.find("/help").expect("/help should render");
    let inbox = text.find("/inbox").expect("/inbox should render");
    let mcp = text.find("/mcp").expect("/mcp should render");
    let ps = text.find("/ps").expect("/ps should render");
    let resume = text.find("/resume").expect("/resume should render");
    let skills = text.find("/skills").expect("/skills should render");
    let subagents = text.find("/subagents").expect("/subagents should render");

    assert!(compact < copy);
    assert!(copy < exit);
    assert!(exit < help);
    assert!(help < inbox);
    assert!(inbox < mcp);
    assert!(mcp < ps);
    assert!(ps < resume);
    assert!(resume < skills);
    assert!(skills < subagents);
}

#[test]
fn tui_state_applies_core_session_events() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你好".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "你好，我在。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    assert_eq!(state.agent_id.as_deref(), Some("agent-a"));
    assert_eq!(state.session_id.as_deref(), Some("session_1234abcd"));
    assert_eq!(state.status_label(), "open");
    assert_eq!(state.message_count, 2);
    assert_eq!(state.turn_count, 1);
    assert!(state.transcript_text().contains("› 你好"));
    assert!(state.transcript_text().contains("\n\n你好，我在。"));
    assert!(!state.transcript_text().contains("user:"));
    assert!(!state.transcript_text().contains("assistant:"));
}

#[test]
fn session_closed_message_includes_session_id() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });

    state.apply_event(SessionEvent::SessionClosed);

    assert!(state
        .transcript_text()
        .contains("Session session_1234abcd closed"));
}

#[test]
fn warning_event_is_visible_in_transcript() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::Warning {
        message: "Maintainer inbox 拉取失败".into(),
    });

    assert!(state
        .transcript_text()
        .contains("Warning: Maintainer inbox 拉取失败"));
}

#[test]
fn slash_commands_are_classified_for_tui_loop() {
    let catalog = super::slash_command::SlashCommandCatalog::default();
    let classify = |raw: &str| super::classify_input(raw, &catalog);
    assert_eq!(classify("/help"), super::InputAction::Help);
    assert_eq!(classify("/inbox"), super::InputAction::Inbox);
    assert_eq!(classify("/mcp"), super::InputAction::Mcp);
    assert_eq!(classify("/ps"), super::InputAction::Ps);
    assert_eq!(classify("/skills"), super::InputAction::Skills);
    assert_eq!(classify("/subagents"), super::InputAction::Subagents);
    assert_eq!(classify("/compact"), super::InputAction::Compact);
    assert_eq!(classify("/copy"), super::InputAction::Copy);
    assert_eq!(classify("/exit"), super::InputAction::Exit);
    assert_eq!(
        classify("!echo hi"),
        super::InputAction::ShellCommand("echo hi".into())
    );
    assert_eq!(
        classify("!  echo hi  "),
        super::InputAction::ShellCommand("echo hi".into())
    );
    assert_eq!(
        classify("!"),
        super::InputAction::ShellCommand(String::new())
    );
    assert_eq!(
        classify("  写一个 hello world  "),
        super::InputAction::Send("  写一个 hello world  ".into())
    );
    assert_eq!(
        classify("/compact now"),
        super::InputAction::Send("/compact now".into())
    );
    assert_eq!(
        classify("/help me"),
        super::InputAction::Send("/help me".into())
    );
    assert_eq!(
        classify("/inbox now"),
        super::InputAction::Send("/inbox now".into())
    );
    assert_eq!(
        classify("/tmp/foo"),
        super::InputAction::Send("/tmp/foo".into())
    );
    assert!(matches!(
        classify("/refresh"),
        super::InputAction::Unknown(_)
    ));
    assert_eq!(
        classify("  /help  "),
        super::InputAction::Send("  /help  ".into())
    );
    assert_eq!(
        classify("/compact\n"),
        super::InputAction::Send("/compact\n".into())
    );
    assert_eq!(
        classify("/exit\n   "),
        super::InputAction::Send("/exit\n   ".into())
    );
    assert_eq!(classify("   "), super::InputAction::Ignore);
}

#[test]
fn skill_slash_commands_are_sent_to_model_as_plain_input() {
    let catalog =
        super::slash_command::SlashCommandCatalog::with_skills([("verify", "运行完整验证")]);
    // 精确命中 skill → 作为普通消息发给模型；带参数时本就归 Send；未知命令仍报 Unknown
    assert_eq!(
        super::classify_input("/verify", &catalog),
        super::InputAction::Send("/verify".into())
    );
    assert_eq!(
        super::classify_input("/verify quick", &catalog),
        super::InputAction::Send("/verify quick".into())
    );
    assert!(matches!(
        super::classify_input("/verif", &catalog),
        super::InputAction::Unknown(_)
    ));
    assert!(matches!(
        super::classify_input("/verif quick", &catalog),
        super::InputAction::Unknown(_)
    ));
    // skill 不遮蔽原生命令
    assert_eq!(
        super::classify_input("/skills", &catalog),
        super::InputAction::Skills
    );
}

#[test]
fn slash_command_echo_renders_as_ui_only_user_entry() {
    let mut state = super::TuiState::new();

    state.push_command_echo("/help".into());
    state.push_help();

    let lines = lines_plain_text(&super::history_render_lines_with_width(&state, 80));
    let text = lines.join("\n");
    let command_index = lines
        .iter()
        .position(|line| line.contains("› /help"))
        .expect("Command echo should render");
    let help_index = lines
        .iter()
        .position(|line| line.contains("ACN commands"))
        .expect("Help output should render");
    assert!(command_index < help_index);
    assert_eq!(blank_lines_between(&lines, command_index, help_index), 1);
    assert!(text.contains("/copy       copy the last Assistant response"));
}

#[test]
fn synchronous_copy_status_keeps_its_existing_single_gap() {
    let mut state = super::TuiState::new();

    state.push_command_echo("/copy".into());
    state.push_system("暂无可复制的 Assistant 回复。");

    let lines = lines_plain_text(&super::history_render_lines_with_width(&state, 80));
    let command_index = lines
        .iter()
        .position(|line| line.contains("› /copy"))
        .expect("/copy echo");
    let status_index = lines
        .iter()
        .position(|line| line.contains("暂无可复制的 Assistant 回复。"))
        .expect("/copy status");
    assert_eq!(blank_lines_between(&lines, command_index, status_index), 1);
}

#[test]
fn flushed_inbox_echo_keeps_gap_before_started_status_while_inbox_is_running() {
    let mut state = super::TuiState::new();

    state.push_command_echo("/inbox".into());
    let command_echo = state.scrollback_lines(80);
    let mut rendered = lines_plain_text(&command_echo.lines);
    state.mark_scrollback_flushed(command_echo.entry_count);

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::SyncingInbox,
    });
    state.apply_event(SessionEvent::InboxStarted);
    let started = state.scrollback_lines(80);
    assert_eq!(
        started.lines.first().map(ToString::to_string).as_deref(),
        Some(""),
        "Inbox started 必须在命令 echo 已 flush 后主动补回空行"
    );
    rendered.extend(lines_plain_text(&started.lines));

    let command_index = rendered
        .iter()
        .position(|line| line.contains("› /inbox"))
        .expect("/inbox echo");
    let started_index = rendered
        .iter()
        .position(|line| line.contains("Inbox started"))
        .expect("Inbox started");
    assert_eq!(
        blank_lines_between(&rendered, command_index, started_index),
        1,
        "Inbox 进行中画面应始终保留一行间隔:\n{}",
        rendered.join("\n")
    );
}

#[test]
fn status_after_flushed_active_user_does_not_add_a_second_gap() {
    let mut state = super::TuiState::new();

    state.begin_pending_turn("普通消息".into());
    let user = state.scrollback_lines(80);
    assert_eq!(
        user.lines.last().map(ToString::to_string).as_deref(),
        Some("")
    );
    state.mark_scrollback_flushed(user.entry_count);

    state.cancel_running_turn("");
    let status = state.scrollback_lines(80);
    assert_eq!(
        status.lines.first().map(ToString::to_string).as_deref(),
        Some("  Turn cancelled"),
        "普通 User 已在上一批保留空行，Status 不能再补第二行"
    );
}

#[test]
fn user_input_after_slash_command_status_keeps_blank_line() {
    let mut state = super::TuiState::new();

    state.push_command_echo("/copy".into());
    state.push_system("暂无可复制的 Assistant 回复。");
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你是谁".into(),
    });

    let text = super::history_render_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        text.contains("暂无可复制的 Assistant 回复。\n\n› 你是谁"),
        "Slash command status should be separated from next user input:\n{text}"
    );
}

#[test]
fn user_input_after_successful_compact_echo_keeps_scrollback_gap() {
    let mut state = super::TuiState::new();

    // 成功 /compact 不会写 status；command echo 已被写入真实 terminal scrollback 后，下一条
    // user entry 必须自己带上开头空行，不能依赖未 flush 的前一条 cell。
    state.push_command_echo("/compact".into());
    let compact_echo = state.scrollback_lines(80);
    state.mark_scrollback_flushed(compact_echo.entry_count);

    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "不要调用工具，报告当前状态".into(),
    });
    let next = state.scrollback_lines(80);
    let lines = next
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(lines.first().map(String::as_str), Some(""));
    assert!(lines
        .get(1)
        .is_some_and(|line| line.contains("不要调用工具，报告当前状态")));
}

#[test]
fn shell_command_entries_keep_blank_lines_between_turns() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "hihi".into(),
    });
    state.apply_event(SessionEvent::UserShellCommandStarted {
        command: "pwd".into(),
    });
    state.apply_event(SessionEvent::UserShellCommandCompleted {
        command: "pwd".into(),
        status: UserShellCommandStatus::Completed,
        exit_code: Some(0),
        duration_ms: 42,
        stdout: "/tmp/project\n".into(),
        stderr: String::new(),
        truncated: false,
        message_count: 1,
    });
    state.apply_event(SessionEvent::UserShellCommandStarted {
        command: "echo great".into(),
    });
    state.apply_event(SessionEvent::UserShellCommandCompleted {
        command: "echo great".into(),
        status: UserShellCommandStatus::Completed,
        exit_code: Some(0),
        duration_ms: 39,
        stdout: "great\n".into(),
        stderr: String::new(),
        truncated: false,
        message_count: 2,
    });
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你是谁".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 80);
    let text_lines = rendered
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let first_shell = text_lines
        .iter()
        .position(|line| line.contains("shell pwd"))
        .expect("First shell command should render");
    let second_shell = text_lines
        .iter()
        .position(|line| line.contains("shell echo great"))
        .expect("Second shell command should render");
    let user = text_lines
        .iter()
        .position(|line| line.contains("› 你是谁"))
        .expect("User turn should render");

    assert!(text_lines[first_shell.saturating_sub(1)].trim().is_empty());
    assert!(text_lines[second_shell.saturating_sub(1)].trim().is_empty());
    assert!(text_lines[user.saturating_sub(1)].trim().is_empty());
}

#[test]
fn shell_in_flight_live_region_uses_shell_copy_after_running_status() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::UserShellCommandStarted {
        command: "zsh -ic 'll ~'".into(),
    });
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });

    let live_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(live_text.contains("Shell · Running command"));
    assert!(live_text.contains("running shell command..."));
    assert!(!live_text.contains("Working · Streaming response"));
    assert!(!live_text.contains("thinking..."));
}

#[test]
fn active_user_after_shell_command_keeps_blank_line_across_live_region() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::UserShellCommandStarted {
        command: "echo great".into(),
    });
    state.apply_event(SessionEvent::UserShellCommandCompleted {
        command: "echo great".into(),
        status: UserShellCommandStatus::Completed,
        exit_code: Some(0),
        duration_ms: 39,
        stdout: "great\n".into(),
        stderr: String::new(),
        truncated: false,
        message_count: 1,
    });
    state.begin_pending_turn("hihi".into());

    let mut text_lines = super::inline_scrollback_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    text_lines.extend(
        super::inline_live_lines_with_width(&state, 80)
            .into_iter()
            .map(|line| line.to_string()),
    );
    let shell = text_lines
        .iter()
        .position(|line| line.contains("shell echo great"))
        .expect("Shell command should render");
    let user = text_lines
        .iter()
        .position(|line| line.contains("› hihi"))
        .expect("Active user turn should render");

    assert!(shell < user);
    assert!(text_lines[user.saturating_sub(1)].trim().is_empty());
}

#[test]
fn resume_reset_drops_temporary_command_echo_before_restored_history() {
    let mut state = super::TuiState::new();
    state.push_command_echo("/resume".into());
    state.push_system("Loading resumable sessions...");

    state.reset_for_resumed_session();
    state.session_id = Some("session_2222bbbb".into());
    state.push_historical_turns(&[HistoricalTurn {
        user_text: "恢复的用户消息".into(),
        assistant_text: Some("恢复的回复".into()),
    }]);
    state.push_system("Session session_2222bbbb resumed.");

    let text = state.transcript_text();
    assert!(!text.contains("/resume"));
    assert!(!text.contains("Loading resumable sessions"));
    assert!(text.contains("恢复的用户消息"));
    assert!(text.contains("恢复的回复"));
    assert!(text.contains("Session session_2222bbbb resumed."));
    assert!(super::composer_hint(&state).starts_with("session_2222bbbb "));
}

#[test]
fn journal_timeline_replay_renders_status_steer_and_tool_state() {
    let mut state = super::TuiState::new();
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "恢复的用户消息".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("半截回复".into()),
        assistant_completed: false,
        status: Some(TurnJournalStatus::InterruptedByUser),
        tool_calls: vec![TurnJournalToolCall {
            tool_use_id: "toolu_1".into(),
            name: "file_read".into(),
            started_summary: "tool file_read path=src/lib.rs".into(),
            input_preview: r#"{"path":"src/lib.rs"}"#.into(),
            input_truncated: false,
            latest_progress: Some("reading".into()),
            completed_summary: Some("tool file_read ok bytes=12".into()),
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            output_preview: Some(r#"{"bytes":12}"#.into()),
            output_truncated: false,
            file_change: None,
        }],
        timeline_items: Vec::new(),
        user_steers: vec!["换个方向".into()],
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let text = state.transcript_text();
    assert!(text.contains("恢复的用户消息"));
    assert!(text.contains("半截回复"));
    assert!(text.contains("Called file_read"));
    assert!(text.contains("path=src/lib.rs"));
    assert!(text.contains("User steer: 换个方向"));
    assert!(text.contains("Turn interrupted_by_user"));
}

#[test]
fn journal_timeline_replay_finalizes_partial_assistant_before_status_lines() {
    let mut state = super::TuiState::new();
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "恢复的用户消息".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("半截回复".into()),
        assistant_completed: false,
        status: Some(TurnJournalStatus::InterruptedByUser),
        tool_calls: Vec::new(),
        timeline_items: vec![TurnJournalTimelineItem::Assistant {
            text: "半截回复".into(),
            completed: false,
        }],
        user_steers: vec!["换个方向".into()],
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let scrollback =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 96)).join("\n");
    let live = lines_plain_text(&super::inline_live_lines_with_width(&state, 96)).join("\n");
    assert!(scrollback.contains("半截回复"));
    assert!(scrollback.contains("User steer: 换个方向"));
    assert!(scrollback.contains("Turn interrupted_by_user"));
    assert!(!live.contains("半截回复"));
}

#[test]
fn resumed_fallback_turn_uses_persisted_exhausted_or_interrupted_detail() {
    let mut exhausted = super::TuiState::new();
    exhausted.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "question".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("partial".into()),
        assistant_completed: false,
        status: Some(TurnJournalStatus::Failed),
        tool_calls: Vec::new(),
        timeline_items: vec![TurnJournalTimelineItem::Assistant {
            text: "partial".into(),
            completed: false,
        }],
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: Some(
            "turn failed after non-streaming retries (5/5): network down".into(),
        ),
    }]);
    let exhausted_text = exhausted.transcript_text();
    assert!(exhausted_text.contains("partial"));
    assert!(exhausted_text.contains("Turn failed after non-streaming retries (5/5): network down"));

    let mut interrupted = super::TuiState::new();
    interrupted.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "question".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("partial".into()),
        assistant_completed: false,
        status: None,
        tool_calls: Vec::new(),
        timeline_items: vec![TurnJournalTimelineItem::Assistant {
            text: "partial".into(),
            completed: false,
        }],
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: Some(
            "turn interrupted during non-streaming fallback (attempt 2/5)".into(),
        ),
    }]);
    let interrupted_text = interrupted.transcript_text();
    assert!(
        interrupted_text.contains("Turn interrupted during non-streaming fallback (attempt 2/5)")
    );
    assert!(!interrupted_text.contains("Turn partial assistant replayed"));
}

#[test]
fn journal_timeline_replay_renders_recovery_notice_explicitly() {
    let mut state = super::TuiState::new();
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "恢复请求".into(),
        canonical_user_content_hash: None,
        assistant_text: None,
        assistant_completed: false,
        status: Some(TurnJournalStatus::InterruptedByUser),
        tool_calls: Vec::new(),
        timeline_items: Vec::new(),
        user_steers: Vec::new(),
        recovery_notice: Some(
            "journal 缺少 assistant timeline；恢复内容的原始相对顺序未知：\ncanonical 回复".into(),
        ),
        turn_status_detail: None,
    }]);

    let text = state.transcript_text();
    assert!(text.contains("Recovery notice:"));
    assert!(text.contains("原始相对顺序未知"));
    assert!(text.contains("canonical 回复"));
}

#[test]
fn committed_journal_timeline_renders_tools_before_final_assistant() {
    let mut state = super::TuiState::new();
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "运行工具".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("最终回复".into()),
        assistant_completed: true,
        status: Some(TurnJournalStatus::Committed),
        tool_calls: vec![TurnJournalToolCall {
            tool_use_id: "toolu_1".into(),
            name: "file_read".into(),
            started_summary: "tool file_read path=src/lib.rs".into(),
            input_preview: r#"{"path":"src/lib.rs"}"#.into(),
            input_truncated: false,
            latest_progress: None,
            completed_summary: Some("tool file_read ok bytes=12".into()),
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            output_preview: Some(r#"{"bytes":12}"#.into()),
            output_truncated: false,
            file_change: None,
        }],
        timeline_items: Vec::new(),
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let text = state.transcript_text();
    let tool_index = text.find("Called file_read").unwrap();
    let assistant_index = text.find("最终回复").unwrap();
    assert!(tool_index < assistant_index);
}

#[test]
fn journal_timeline_replay_preserves_assistant_tool_assistant_order() {
    let mut state = super::TuiState::new();
    let tool = TurnJournalToolCall {
        tool_use_id: "toolu_1".into(),
        name: "file_read".into(),
        started_summary: "tool file_read path=src/lib.rs".into(),
        input_preview: r#"{"path":"src/lib.rs"}"#.into(),
        input_truncated: false,
        latest_progress: None,
        completed_summary: Some("tool file_read ok bytes=12".into()),
        interrupted_summary: None,
        skipped_summary: None,
        skip_reason: None,
        outcome: None,
        output_preview: Some(r#"{"bytes":12}"#.into()),
        output_truncated: false,
        file_change: None,
    };
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "运行工具".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("我先读一下。最终回复".into()),
        assistant_completed: true,
        status: Some(TurnJournalStatus::Committed),
        tool_calls: vec![tool.clone()],
        timeline_items: vec![
            TurnJournalTimelineItem::Assistant {
                text: "我先读一下。".into(),
                completed: true,
            },
            TurnJournalTimelineItem::ToolCall(Box::new(tool)),
            TurnJournalTimelineItem::Assistant {
                text: "最终回复".into(),
                completed: true,
            },
        ],
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let text = state.transcript_text();
    let pre_index = text.find("我先读一下。").unwrap();
    let tool_index = text.find("Called file_read").unwrap();
    let final_index = text.find("最终回复").unwrap();
    assert!(pre_index < tool_index);
    assert!(tool_index < final_index);
}

#[test]
fn inbox_events_render_status_summary() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::SyncingInbox,
    });
    state.apply_event(SessionEvent::InboxStarted);
    assert_eq!(state.status, SessionRuntimeStatus::SyncingInbox);
    assert_eq!(state.status_label(), "syncing inbox");
    assert!(state.transcript_text().contains("Inbox started"));
    assert!(super::history_render_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("syncing inbox")));
    assert!(super::inline_live_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("Inbox · Syncing updates")));

    state.apply_event(SessionEvent::InboxCompleted {
        processed: 2,
        new_claim_ids: vec!["claim_00000001".parse().unwrap()],
        updated_claim_ids: vec!["claim_00000002".parse().unwrap()],
        new_dispute_ids: vec!["dispute_00000001".parse().unwrap()],
        deprecated_claim_ids: vec![],
    });

    let text = state.transcript_text();
    assert!(text.contains("Inbox completed: processed=2"));
    assert!(text.contains("new_claims=1"));
    assert!(text.contains("updated_claims=1"));
    assert!(text.contains("new_disputes=1"));
}

#[test]
fn multiline_compact_like_input_is_submitted_as_plain_input() {
    let input = "/compact\n继续说明";

    assert_eq!(
        super::classify_input(input, &Default::default()),
        super::InputAction::Send(input.into())
    );
}

#[test]
fn slash_like_source_code_is_submitted_as_plain_input() {
    let code = "//! Provider adapter 实现：通过统一协议调用模型。\n\
//! 用途：\n\
//! - 交互 session 通过 provider-neutral adapter 调用模型\n\
\n\
use async_trait::async_trait;";

    assert_eq!(
        super::classify_input(code, &Default::default()),
        super::InputAction::Send(code.into())
    );
}

#[test]
fn compaction_progress_is_visible() {
    let mut state = super::TuiState::new();

    state.apply_event(SessionEvent::CompactionStarted {
        compact_start_index: 0,
        compact_end_index: 2,
        recap_start_index: 0,
        recap_end_index: 4,
    });
    assert_eq!(state.status, SessionRuntimeStatus::Compacting);
    assert_eq!(state.status_label(), "compacting");
    assert!(super::inline_live_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("Compacting · Session history")));
    let compacting_text = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!compacting_text
        .lines()
        .any(|line| line.contains("┆ ") && line.contains("thinking")));
    assert!(!compacting_text.contains("compacting session"));
    assert!(!state.transcript_text().contains("compaction started"));

    state.apply_event(SessionEvent::CompactionCompleted {
        compacted_until: 2,
        recapped_until: 4,
        new_claim_ids: vec!["claim_00000001".parse().unwrap()],
        updated_claim_ids: vec![],
        new_dispute_ids: vec![],
    });
    assert_eq!(state.status, SessionRuntimeStatus::Compacting);
    assert!(!state.transcript_text().contains("compaction completed"));

    state.apply_event(SessionEvent::CompactionFailed {
        error: "boom".into(),
    });
    assert_eq!(state.status, SessionRuntimeStatus::Error);
    assert!(state.transcript_text().contains("Compaction failed: boom"));
}

#[test]
fn repeated_compaction_failures_render_actionable_messages_without_generic_prefixes() {
    let mut manual = super::TuiState::new();
    let manual_message =
        "Compaction failed repeatedly. You can run /compact to try again or start a new session.";
    manual.apply_event(SessionEvent::CompactionFailed {
        error: manual_message.into(),
    });
    assert!(manual.transcript_text().contains(manual_message));
    assert!(!manual
        .transcript_text()
        .contains("Compaction failed: Compaction failed repeatedly."));

    let mut automatic = super::TuiState::new();
    automatic.begin_pending_turn("continue".into());
    let automatic_message = "Context compaction failed: the generated summary exceeded 40,000 characters after 2 attempts. Run /compact to retry.";
    automatic.fail_running_turn(automatic_message);
    assert!(automatic.transcript_text().contains(automatic_message));
    assert!(!automatic
        .transcript_text()
        .contains("Turn failed: Context compaction failed:"));
}

#[test]
fn manual_compaction_warning_and_error_keep_single_gaps_across_scrollback() {
    let mut state = super::TuiState::new();
    state.push_command_echo("/compact".into());

    let command = state.scrollback_lines(120);
    let mut rendered = lines_plain_text(&command.lines);
    state.mark_scrollback_flushed(command.entry_count);

    state.apply_event(SessionEvent::Warning {
        message: "compaction summary JSON invalid, retrying (1/1): summary too long".into(),
    });
    let warning = state.scrollback_lines(120);
    rendered.extend(lines_plain_text(&warning.lines));
    state.mark_scrollback_flushed(warning.entry_count);

    state.apply_event(SessionEvent::CompactionFailed {
        error:
            "Compaction failed repeatedly. You can run /compact to try again or start a new session."
                .into(),
    });
    rendered.extend(lines_plain_text(&state.scrollback_lines(120).lines));

    let command_index = rendered
        .iter()
        .position(|line| line.contains("› /compact"))
        .expect("compact command echo");
    let warning_index = rendered
        .iter()
        .position(|line| line.contains("Warning: compaction summary JSON invalid"))
        .expect("compaction retry warning");
    let error_index = rendered
        .iter()
        .position(|line| line.contains("Error Compaction failed repeatedly"))
        .expect("compaction failure");

    assert_eq!(
        blank_lines_between(&rendered, command_index, warning_index),
        1
    );
    assert_eq!(
        blank_lines_between(&rendered, warning_index, error_index),
        1
    );
}

#[test]
fn automatic_compaction_warning_keeps_gap_before_assistant_across_scrollback() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("请继续".into());

    let user = state.scrollback_lines(120);
    let mut rendered = lines_plain_text(&user.lines);
    state.mark_scrollback_flushed(user.entry_count);

    state.apply_event(SessionEvent::Warning {
        message: "Automatic compaction failed after 2 attempts; continuing with full history."
            .into(),
    });

    let warning_frame = lines_plain_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(warning_frame
        .iter()
        .any(|line| line.contains("Warning: Automatic compaction failed after 2 attempts")));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "RAW_FALLBACK_OK".into(),
    });

    let live = lines_plain_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(live
        .iter()
        .any(|line| line.contains("Warning: Automatic compaction failed after 2 attempts")));
    assert!(live.iter().any(|line| line.contains("RAW_FALLBACK_OK")));

    let timeline = lines_plain_text(&state.active_assistant_lines(120));
    let timeline_warning_index = timeline
        .iter()
        .position(|line| line.contains("Warning: Automatic compaction failed after 2 attempts"))
        .expect("timeline automatic compaction warning");
    let timeline_assistant_index = timeline
        .iter()
        .position(|line| line.contains("RAW_FALLBACK_OK"))
        .expect("timeline Assistant response");
    assert_eq!(
        blank_lines_between(&timeline, timeline_warning_index, timeline_assistant_index),
        1,
        "streaming Assistant 必须与 live Warning 保持一行间隔"
    );

    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    rendered.extend(lines_plain_text(&state.scrollback_lines(120).lines));

    let warning_index = rendered
        .iter()
        .position(|line| line.contains("Automatic compaction failed after 2 attempts"))
        .expect("automatic compaction warning");
    let assistant_index = rendered
        .iter()
        .position(|line| line.contains("RAW_FALLBACK_OK"))
        .expect("assistant response");

    assert_eq!(
        blank_lines_between(&rendered, warning_index, assistant_index),
        1
    );
    assert_eq!(
        rendered
            .iter()
            .filter(|line| line.contains("Warning: Automatic compaction failed after 2 attempts"))
            .count(),
        1,
        "提交后 Warning 只能写入一次 scrollback"
    );
    assert!(!rendered
        .iter()
        .any(|line| line.contains("No context was discarded.")));
}

#[test]
fn assistant_completed_replaces_streaming_delta_line() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantTextDelta { text: "hel".into() });
    state.apply_event(SessionEvent::AssistantTextDelta { text: "lo".into() });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "hello".into(),
    });

    assert_eq!(state.transcript_text(), "hello");
}

#[test]
fn non_streaming_fallback_keeps_partial_then_replaces_it_and_restores_thinking() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("question".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "partial answer".into(),
    });
    state.apply_event(SessionEvent::NonStreamingFallbackAttemptStarted {
        attempt: 1,
        max_attempts: 5,
    });

    let retrying = lines_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(retrying.contains("partial answer"));
    assert!(retrying.contains("Falling Back to non-streaming · Retrying 1/5..."));

    state.apply_event(SessionEvent::NonStreamingFallbackSucceeded {
        text: "complete replacement".into(),
    });

    let completed = lines_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(completed.contains("complete replacement"));
    assert!(!completed.contains("partial answer"));
    assert!(completed.contains("thinking..."));
    assert!(!completed.contains("Falling Back to non-streaming"));
}

#[test]
fn tool_only_non_streaming_fallback_success_clears_partial() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("question".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "partial answer".into(),
    });
    state.apply_event(SessionEvent::NonStreamingFallbackAttemptStarted {
        attempt: 1,
        max_attempts: 5,
    });
    state.apply_event(SessionEvent::NonStreamingFallbackSucceeded {
        text: String::new(),
    });

    let completed = lines_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(!completed.contains("partial answer"));
    assert!(completed.contains("thinking..."));
}

#[test]
fn exhausted_non_streaming_fallback_keeps_partial_and_uses_existing_failure_cell() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("question".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "partial answer".into(),
    });
    state.apply_event(SessionEvent::NonStreamingFallbackAttemptStarted {
        attempt: 5,
        max_attempts: 5,
    });
    state.fail_running_turn("non-streaming fallback exhausted after 5/5: network down");

    let text = state.transcript_text();
    assert!(text.contains("partial answer"));
    assert!(text.contains("Turn failed: non-streaming fallback exhausted after 5/5: network down"));
    let live = lines_text(&super::inline_live_lines_with_width(&state, 120));
    assert!(!live.contains("Falling Back to non-streaming"));
}

#[test]
fn last_committed_assistant_text_ignores_uncommitted_streaming_turn() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一问".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一答".into(),
    });
    assert_eq!(state.last_committed_assistant_text(), None);

    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    assert_eq!(state.last_committed_assistant_text(), Some("第一答"));

    state.begin_pending_turn("第二问".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "第二答 streaming".into(),
    });
    assert_eq!(state.last_committed_assistant_text(), Some("第一答"));
}

#[test]
fn session_metadata_stays_out_of_conversation_history() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });

    assert_eq!(state.transcript_text(), "");
}

#[test]
fn tool_completion_updates_the_active_tool_entry() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "memory".into(),
        summary: "tool memory add".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_1".into(),
        summary: "tool memory ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let transcript = state.transcript_text();
    assert!(transcript.contains("Called memory"));
    assert!(!transcript.contains("  └ ok"));
    assert!(!transcript.contains("tool memory add"));
}

#[test]
fn interrupted_wait_tool_is_finalized_and_does_not_block_later_turns() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("等待子代理".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_wait".into(),
        name: "wait_subagents".into(),
        summary: r#"tool wait_subagents {"until":"all_terminal"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallInterrupted {
        id: "toolu_wait".into(),
        summary: "tool wait_subagents interrupted".into(),
    });
    state.apply_event(SessionEvent::TurnInterrupted {
        reason: "user steer pending".into(),
    });

    state.begin_pending_turn("停止等待，继续回答".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "已经停止等待。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    let scrollback = state.scrollback_lines(96);
    let text = lines_plain_text(&scrollback.lines).join("\n");
    assert!(text.contains("Interrupted wait_subagents"));
    assert!(text.contains("停止等待，继续回答"));
    assert!(text.contains("已经停止等待。"));
    assert!(!text.contains("Calling wait_subagents"));
    assert!(!text.contains("elapsed"));
}

#[test]
fn skipped_tool_is_finalized_without_calling_or_elapsed_and_allows_scrollback() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("读取文件".into());
    state.apply_event(SessionEvent::ToolCallSkipped {
        id: "toolu_read".into(),
        name: "file_read".into(),
        summary: r#"tool file_read {"path":"src/lib.rs"}"#.into(),
        reason: ToolCallSkipReason::TurnCancelledBeforeDispatch,
    });
    state.apply_event(SessionEvent::TurnCancelled {
        reason: "user cancelled turn".into(),
    });

    state.begin_pending_turn("继续回答".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "已经继续。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    let scrollback = state.scrollback_lines(96);
    let text = lines_plain_text(&scrollback.lines).join("\n");
    assert!(text.contains("Skipped file_read"));
    assert!(text.contains("Turn cancelled before dispatch"));
    assert!(text.contains("继续回答"));
    assert!(text.contains("已经继续。"));
    assert!(!text.contains("Calling file_read"));
    assert!(!text.contains("elapsed"));
}

#[test]
fn resumed_skipped_tool_does_not_degrade_to_journal_replay_failure() {
    let mut state = super::TuiState::new();
    let tool = TurnJournalToolCall {
        tool_use_id: "toolu_read".into(),
        name: "file_read".into(),
        started_summary: String::new(),
        input_preview: r#"{"path":"src/lib.rs"}"#.into(),
        input_truncated: false,
        latest_progress: None,
        completed_summary: None,
        interrupted_summary: None,
        skipped_summary: Some(r#"tool file_read {"path":"src/lib.rs"}"#.into()),
        skip_reason: Some(ToolCallSkipReason::TurnInterruptedBeforeDispatch),
        outcome: None,
        output_preview: None,
        output_truncated: false,
        file_change: None,
    };
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "读取文件".into(),
        canonical_user_content_hash: None,
        assistant_text: None,
        assistant_completed: false,
        status: Some(TurnJournalStatus::InterruptedByUser),
        tool_calls: vec![tool.clone()],
        timeline_items: vec![TurnJournalTimelineItem::ToolCall(Box::new(tool))],
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let text = state.transcript_text();
    assert!(text.contains("Skipped file_read"));
    assert!(text.contains("Turn interrupted before dispatch"));
    assert!(!text.contains("Calling file_read"));
    assert!(!text.contains("Journal replay incomplete"));
}

#[test]
fn composer_supports_left_right_insert_and_delete() {
    let mut state = super::TuiState::new();
    state.push_input_char('你');
    state.push_input_char('好');
    state.move_input_left();
    state.push_input_char('很');
    assert_eq!(state.input(), "你很好");
    assert_eq!(super::composer_cursor_x(&state, 0), 6);

    state.move_input_right();
    state.pop_input_char();
    assert_eq!(state.input(), "你很");
    state.move_input_home();
    state.delete_input_char();
    assert_eq!(state.input(), "很");
}

#[test]
fn assistant_markdown_renders_structured_lines() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "# 标题\n\n- **重点** `code`\n\n```python\nprint(1)\n```".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 80);
    assert!(rendered
        .iter()
        .any(|line| line.to_string().contains("标题")));
    assert!(rendered
        .iter()
        .any(|line| line.to_string().contains("• 重点 code")));
    assert!(rendered
        .iter()
        .any(|line| line.to_string().contains("╭─ python")));
    assert!(!rendered
        .iter()
        .any(|line| line.to_string().contains("```python")));
}

#[test]
fn active_tool_cell_renders_calling_header_and_input_detail() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "file_read".into(),
        summary: "tool file_read {\"path\":\"src/lib.rs\"}".into(),
    });

    let transcript = state.transcript_text();
    assert!(transcript.contains("Calling file_read"));
    assert!(transcript.contains("{\"path\":\"src/lib.rs\"}"));
    assert!(!transcript.contains("tool file_read"));
}

#[test]
fn multiline_user_and_following_tool_keep_single_blank_line() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "第一行\n第二行\n".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "create_subagent".into(),
        summary: r#"tool create_subagent {"title":"manual-happy-1"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_1".into(),
        summary: "tool create_subagent ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let lines = lines_plain_text(&super::history_render_lines_with_width(&state, 96));
    let user_index = lines
        .iter()
        .rposition(|line| line.contains("第二行"))
        .expect("User input should render");
    let tool_index = lines
        .iter()
        .position(|line| line.contains("Called create_subagent"))
        .expect("Tool should render");

    assert_eq!(
        blank_lines_between(&lines, user_index, tool_index),
        1,
        "User and following tool should have one blank line:\n{}",
        lines.join("\n")
    );
}

#[test]
fn consecutive_completed_tools_keep_single_blank_line() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查 subagent 状态".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_list".into(),
        name: "list_subagents".into(),
        summary: "tool list_subagents {}".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_list".into(),
        summary: "tool list_subagents ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_read".into(),
        name: "read_subagent".into(),
        summary: r#"tool read_subagent {"id":"subagent_12345678","mode":"summary"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_read".into(),
        summary: "tool read_subagent ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let lines = lines_plain_text(&state.active_assistant_lines(96));
    let list_detail_index = lines
        .iter()
        .position(|line| line.contains("└ {}"))
        .expect("List_subagents input should render");
    let read_index = lines
        .iter()
        .position(|line| line.contains("Called read_subagent"))
        .expect("Read_subagent should render");

    assert_eq!(
        blank_lines_between(&lines, list_detail_index, read_index),
        1,
        "Consecutive tools should have one blank line:\n{}",
        lines.join("\n")
    );
}

#[test]
fn wait_subagents_uses_the_regular_tool_history_cell_with_single_spacing() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "等子代理完成再继续".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_wait".into(),
        name: "wait_subagents".into(),
        summary: r#"tool wait_subagents {"until":"all_terminal"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_wait".into(),
        summary: "tool wait_subagents ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let lines = lines_plain_text(&super::history_render_lines_with_width(&state, 96));
    let user_index = lines
        .iter()
        .position(|line| line.contains("等子代理完成再继续"))
        .expect("User input should render");
    let tool_index = lines
        .iter()
        .position(|line| line.contains("Called wait_subagents"))
        .expect("Wait_subagents should render as an ordinary completed tool");
    assert_eq!(blank_lines_between(&lines, user_index, tool_index), 1);
}

#[test]
fn flushed_user_and_later_committed_tool_keep_single_blank_line() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();
    state.begin_pending_turn("不要废话直接告诉我今天上海天气  工具调用前别说话".into());

    let first_flush = state.scrollback_lines(120);
    let mut accumulated = lines_plain_text(&first_flush.lines);
    state.mark_scrollback_flushed(first_flush.entry_count);

    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_web".into(),
        name: "web_search".into(),
        summary: r#"tool web_search {"query":"上海天气 2026年7月8日"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_web".into(),
        summary: "tool web_search ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "今天上海天气很热。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    let second_flush = state.scrollback_lines(120);
    accumulated.extend(lines_plain_text(&second_flush.lines));
    let user_index = accumulated
        .iter()
        .position(|line| line.contains("工具调用前别说话"))
        .expect("User input should render");
    let tool_index = accumulated
        .iter()
        .position(|line| line.contains("Called web_search"))
        .expect("Tool should render");

    assert_eq!(
        blank_lines_between(&accumulated, user_index, tool_index),
        1,
        "Flushed user and later committed tool should have one blank line:\n{}",
        accumulated.join("\n")
    );
}

#[test]
fn failed_tool_cell_renders_error_detail() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "file_read".into(),
        summary: "tool file_read {\"path\":\"missing\"}".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_1".into(),
        summary: "tool file_read failed permission denied".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::BusinessFailure,
    });

    let transcript = state.transcript_text();
    assert!(transcript.contains("Called file_read"));
    assert!(transcript.contains("{\"path\":\"missing\"}"));
    assert!(transcript.contains("Error: permission denied"));
}

#[test]
fn composer_cursor_position_counts_wide_chars() {
    let mut state = super::TuiState::new();
    state.push_input_char('你');
    state.push_input_char('好');
    assert_eq!(super::composer_cursor_x(&state, 0), 6);
}

#[test]
fn shift_enter_is_classified_as_input_newline() {
    let shifted_enter = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Enter,
        crossterm::event::KeyModifiers::SHIFT,
    );
    let plain_enter = crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Enter,
        crossterm::event::KeyModifiers::NONE,
    );

    assert!(super::is_shift_enter_newline(shifted_enter));
    assert!(!super::is_shift_enter_newline(plain_enter));
}

#[test]
fn composer_newline_renders_continuation_line_and_moves_cursor() {
    let mut state = super::TuiState::new();
    for c in "第一行".chars() {
        state.push_input_char(c);
    }
    state.push_input_newline();
    for c in "第二行".chars() {
        state.push_input_char(c);
    }

    assert_eq!(state.input(), "第一行\n第二行");
    assert_eq!(super::composer_height(&state), 3);
    assert_eq!(super::composer_cursor_x(&state, 0), 8);
    assert_eq!(super::composer_cursor_y(&state, 0), 1);

    let rendered = super::composer_lines_with_width(&state, 16)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert!(rendered[0].contains("› 第一行"));
    assert!(rendered[1].contains("  第二行"));
}

#[test]
fn composer_wraps_long_input_to_terminal_width() {
    let mut state = super::TuiState::new();
    state.push_input_text("abcdefghijklmnopqrstuvwxyz");

    let rendered = super::composer_lines_with_width(&state, 10)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(super::composer_height_for_width(&state, 10), 5);
    assert_eq!(super::composer_cursor_x_for_width(&state, 0, 10), 4);
    assert_eq!(super::composer_cursor_y_for_width(&state, 0, 10), 3);
    assert!(rendered[0].contains("› abcdefgh"));
    assert!(rendered[1].contains("  ijklmnop"));
    assert!(rendered[2].contains("  qrstuvwx"));
    assert!(rendered[3].contains("  yz"));
}

#[test]
fn composer_wrap_keeps_cursor_on_visible_tail() {
    let mut state = super::TuiState::new();
    state.push_input_text(&"x".repeat(80));

    let rendered = super::composer_lines_with_width(&state, 12)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(super::composer_height_for_width(&state, 12), 9);
    assert_eq!(super::composer_cursor_y_for_width(&state, 0, 12), 7);
    assert_eq!(rendered.len(), 9);
    assert!(rendered[0].contains("xxxxxxxxxx"));
    assert!(rendered[7].contains("xxxxxxxxxx"));
}

#[test]
fn long_multiline_composer_height_is_capped_and_keeps_tail_visible() {
    let mut state = super::TuiState::new();
    let input = (0..20)
        .map(|index| format!("line-{index:02}"))
        .collect::<Vec<_>>()
        .join("\n");

    state.push_input_text(&input);

    assert_eq!(super::composer_height(&state), 9);
    assert_eq!(super::composer_cursor_y(&state, 0), 7);
    let rendered = super::composer_lines_with_width(&state, 20)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert_eq!(rendered.len(), 9);
    assert!(rendered[0].contains("line-12"));
    assert!(rendered[7].contains("line-19"));
}

#[test]
fn composer_trailing_newline_keeps_empty_line_background() {
    let mut state = super::TuiState::new();
    for c in "你好".chars() {
        state.push_input_char(c);
    }
    state.push_input_newline();

    let lines = super::composer_lines_with_width(&state, 16);

    assert_eq!(lines[0].width(), 16);
    assert_eq!(lines[1].width(), 16);
    assert_eq!(
        lines[1].style.bg,
        Some(ratatui::style::Color::Rgb(226, 224, 219))
    );
    assert_eq!(
        lines[1].spans.last().unwrap().style.bg,
        Some(ratatui::style::Color::Rgb(226, 224, 219))
    );
}

#[test]
fn composer_render_keeps_newline_rows_in_the_input_bar() {
    let mut state = super::TuiState::new();
    state.push_input_char('和');
    state.push_input_newline();
    state.push_input_newline();

    let lines = super::composer_lines_with_width(&state, 16);

    for line in lines.iter().take(3) {
        assert_eq!(
            line.style.bg,
            Some(ratatui::style::Color::Rgb(226, 224, 219))
        );
    }
}

#[test]
fn submitted_user_echo_with_blank_line_keeps_rows_in_gray_bar() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你好\n\n我是".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 16);

    assert_eq!(rendered[0].style.bg, Some(ratatui::style::Color::Gray));
    assert_eq!(rendered[1].style.bg, Some(ratatui::style::Color::Gray));
    assert_eq!(rendered[2].style.bg, Some(ratatui::style::Color::Gray));
    assert_eq!(rendered.len(), 3);
}

#[test]
fn composer_wrap_keeps_grapheme_clusters_together() {
    let mut state = super::TuiState::new();
    state.push_input_text("e\u{301}e\u{301}e\u{301}");

    let rendered = super::composer_lines_with_width(&state, 3)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered.iter().all(|line| !line.contains("› \u{301}")));
    assert!(rendered.iter().all(|line| !line.contains("  \u{301}")));
    assert!(rendered.iter().any(|line| line.contains("e\u{301}")));
}

#[test]
fn submitted_user_echo_wrap_keeps_grapheme_clusters_together() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "e\u{301}e\u{301}e\u{301}".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 3)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered.iter().all(|line| !line.contains("› \u{301}")));
    assert!(rendered.iter().all(|line| !line.contains("  \u{301}")));
    assert!(rendered.iter().any(|line| line.contains("e\u{301}")));
}

#[test]
fn submitted_user_echo_wraps_to_transcript_width() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "abcdefghijklmnopqrstuvwxyz".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 10)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered[0].contains("› abcdefgh"));
    assert!(rendered[1].contains("  ijklmnop"));
    assert!(rendered[2].contains("  qrstuvwx"));
    assert!(rendered[3].contains("  yz"));
}

#[test]
fn assistant_chinese_text_hard_wraps_to_transcript_width() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "德比斯1992年生，11岁开启赛车生涯，16岁获法国125GP冠军。".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 20);

    assert!(rendered.len() > 2);
    assert!(rendered.iter().all(|line| line.width() <= 20));
}

#[test]
fn assistant_unbroken_long_word_hard_wraps_to_transcript_width() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "abcdefghijklmnopqrstuvwxyz".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 8);

    assert!(rendered.len() > 3);
    assert!(rendered.iter().all(|line| line.width() <= 8));
}

#[test]
fn assistant_markdown_wrap_keeps_grapheme_clusters_together() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "e\u{301}e\u{301}e\u{301}".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 2)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered.iter().all(|line| line != "\u{301}"));
    assert!(rendered.iter().any(|line| line.contains("e\u{301}")));
}

#[test]
fn assistant_markdown_wrap_preserves_span_styles() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "**abcdefghijklmnop**".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 6);

    assert!(rendered.len() >= 3);
    assert!(rendered.iter().all(|line| line.width() <= 6));
    assert!(rendered
        .iter()
        .flat_map(|line| line.spans.iter())
        .any(|span| span
            .style
            .add_modifier
            .contains(ratatui::style::Modifier::BOLD)));
}

#[test]
fn pasted_or_composed_text_is_inserted_into_composer() {
    let mut state = super::TuiState::new();

    state.push_input_text("你好");

    assert_eq!(state.input(), "你好");
    assert!(super::composer_lines_with_width(&state, 16)[0]
        .to_string()
        .contains("你好"));
}

#[test]
fn composer_input_renders_as_high_contrast_gray_bar() {
    let mut state = super::TuiState::new();
    state.push_input_char('你');

    let lines = super::composer_lines_with_width(&state, 16);

    assert_eq!(
        lines[0].style.fg,
        Some(ratatui::style::Color::Rgb(35, 35, 33))
    );
    assert_eq!(
        lines[0].style.bg,
        Some(ratatui::style::Color::Rgb(226, 224, 219))
    );
    assert_eq!(lines[0].width(), 16);
    assert_eq!(
        lines[0].spans.last().unwrap().style.bg,
        Some(ratatui::style::Color::Rgb(226, 224, 219))
    );
}

#[test]
fn finalized_history_moves_out_of_inline_live_region() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你好".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "你好，我在。".into(),
    });

    let mut scrollback = state.scrollback_lines(80);
    state.mark_scrollback_flushed(scrollback.entry_count);
    let next_scrollback = state.scrollback_lines(80);
    assert_eq!(next_scrollback.entry_count, 0);
    scrollback.lines.extend(next_scrollback.lines);
    let live = super::inline_live_lines_with_width(&state, 80);
    let scrollback_text = scrollback
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let live_text = live
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback_text.contains("› 你好"));
    assert!(scrollback_text.contains("你好，我在。"));
    assert!(!live_text.contains("你好，我在。"));
}

#[test]
fn history_render_keeps_blank_line_before_user_after_assistant_reply() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "hihi".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "嗨嗨！我在呢。".into(),
    });
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你是谁".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let assistant_index = rendered
        .iter()
        .position(|line| line.contains("嗨嗨！我在呢。"))
        .expect("Assistant reply should render");
    let next_user_index = rendered
        .iter()
        .position(|line| line.contains("› 你是谁"))
        .expect("Next user message should render");

    assert!(assistant_index < next_user_index);
    assert!(rendered[next_user_index.saturating_sub(1)].is_empty());
}

#[test]
fn scrollback_flush_preserves_blank_line_before_next_user_after_assistant() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "hihi".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "嗨嗨！我在呢。".into(),
    });

    let first_scrollback = state.scrollback_lines(80);
    state.mark_scrollback_flushed(first_scrollback.entry_count);
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你是谁".into(),
    });

    let next_scrollback = state.scrollback_lines(80);
    let rendered = next_scrollback
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(rendered.first().map(String::as_str), Some(""));
    assert!(rendered.iter().any(|line| line.contains("› 你是谁")));
}

#[test]
fn scrollback_flush_preserves_blank_line_before_attach_error_after_assistant() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "hihi".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "嗨嗨！我在呢。".into(),
    });

    let first_scrollback = state.scrollback_lines(80);
    state.mark_scrollback_flushed(first_scrollback.entry_count);
    state
        .push_error("Attach failed: 附件路径是目录而不是文件: /Users/example/Desktop/".to_string());

    let rendered = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(rendered.first().map(String::as_str), Some(""));
    assert!(rendered
        .iter()
        .any(|line| line.contains("Error Attach failed: 附件路径是目录而不是文件")));
}

#[test]
fn failed_local_submission_echoes_input_then_error_and_keeps_recall_history() {
    let mut state = super::TuiState::new();
    let attempted = "@/Users/example/Desktop/ 这里有什么内容";
    state.push_input_text(attempted);
    let draft = state.take_input_draft();

    state.record_submitted_draft(draft.clone());
    state.push_failed_input(
        draft.visible_text().to_string(),
        "Attach failed: 附件路径是目录而不是文件: /Users/example/Desktop/",
    );

    assert_eq!(state.input(), "");
    let rendered = super::history_render_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let user_index = rendered
        .iter()
        .position(|line| line.contains(attempted))
        .expect("失败的用户输入应在历史区回显");
    let error_index = rendered
        .iter()
        .position(|line| line.contains("Error Attach failed"))
        .expect("附件错误应在历史区显示");
    assert_eq!(error_index, user_index.saturating_add(2));
    assert!(rendered[user_index.saturating_add(1)].is_empty());

    assert!(state.recall_previous_input());
    assert_eq!(state.input(), attempted);
}

#[test]
fn active_user_prompt_flush_keeps_blank_line_after_previous_assistant() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "hihi".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "嗨嗨！我在呢。".into(),
    });
    let previous = state.scrollback_lines(80);
    state.mark_scrollback_flushed(previous.entry_count);
    state.begin_pending_turn("你是谁".into());

    let scrollback = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let user_index = scrollback
        .iter()
        .position(|line| line.contains("› 你是谁"))
        .expect("Active user prompt should flush to scrollback");

    assert!(user_index > 0);
    assert!(scrollback[user_index - 1].is_empty());
}

#[test]
fn resize_reflows_flushed_history_at_new_width() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "你好".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "你好，我在。".into(),
    });

    let initial = state.scrollback_lines(96);
    state.mark_scrollback_flushed(initial.entry_count);

    // resize 走 hard_clear 全屏重排：reset_flushed 后，整段历史按新宽度重新 emit，
    // 配合 terminal 的 Purge 清掉旧宽度副本，避免重复行 / 渲染紊乱。
    state.reset_flushed_for_hard_clear();
    let resized = super::inline_scrollback_lines_with_width(&state, 56)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let joined = resized.join("\n");
    assert!(joined.contains("› 你好"), "历史应按新宽度重排重发");
    assert!(joined.contains("你好，我在。"));
    assert!(
        resized.iter().all(|line| line.chars().count() <= 56),
        "重排后每行宽度应不超过新终端宽度"
    );
}

#[test]
fn active_assistant_delta_stays_in_inline_live_region_until_completed() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantTextDelta { text: "hel".into() });
    state.apply_event(SessionEvent::AssistantTextDelta { text: "lo".into() });

    assert_eq!(state.scrollback_lines(80).entry_count, 0);
    let live_text = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(live_text.contains("hello"));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "hello".into(),
    });
    let scrollback_text = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(scrollback_text.contains("hello"));
}

#[test]
fn running_live_box_uses_configured_visual_row_limit() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (1..=8)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let live = super::inline_live_lines_with_size_and_preview_max(&state, 96, 40, 5);
    let plain = lines_plain_text(&live);
    let text = plain.join("\n");

    assert!(text.contains("  ..."));
    assert!(!text.contains("line 4"));
    assert!(text.contains("line 5"));
    assert!(text.contains("line 8"));
    assert_eq!(
        live_box_content_line_count(&plain, "Working · Streaming response"),
        5,
        "顶部标题边框和底部边框不计入配置的五行"
    );
}

#[test]
fn configured_visual_row_limit_applies_to_every_live_box_status() {
    let cases = [
        (
            SessionRuntimeStatus::Initializing,
            "Initializing · Syncing inbox",
        ),
        (
            SessionRuntimeStatus::Running,
            "Working · Streaming response",
        ),
        (
            SessionRuntimeStatus::SyncingInbox,
            "Inbox · Syncing updates",
        ),
        (
            SessionRuntimeStatus::Compacting,
            "Compacting · Session history",
        ),
        (
            SessionRuntimeStatus::Finalizing,
            "Finalizing · Committing contribution",
        ),
        (SessionRuntimeStatus::Open, "Idle"),
        (SessionRuntimeStatus::Error, "Attention · Last turn failed"),
        (SessionRuntimeStatus::Closed, "Session closed"),
    ];

    for (status, title) in cases {
        let mut state = super::TuiState::new();
        state.begin_pending_turn("验证所有虚线框状态".into());
        state.apply_event(SessionEvent::AssistantTextDelta {
            text: (1..=12)
                .map(|index| format!("status line {index}"))
                .collect::<Vec<_>>()
                .join("\n"),
        });
        state.apply_event(SessionEvent::StatusChanged { status });

        let live = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
            &state, 96, 40, 5,
        ));
        assert_eq!(
            live_box_content_line_count(&live, title),
            5,
            "{status:?} 应使用同一个框内视觉行预算:\n{}",
            live.join("\n")
        );
        assert!(live.join("\n").contains("status line 12"));
    }
}

#[test]
fn running_live_response_preview_recomputes_after_terminal_resize() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (1..=12)
            .map(|index| format!("resize line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let narrow = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
        &state, 96, 12, 8,
    ));
    let narrow_count = live_box_content_line_count(&narrow, "Working · Streaming response");
    let wide = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
        &state, 96, 40, 8,
    ));
    let wide_count = live_box_content_line_count(&wide, "Working · Streaming response");

    assert!(narrow_count < 8, "小终端应按可用高度临时压缩");
    assert_eq!(wide_count, 8, "终端变大后应恢复到配置上限");
    assert!(wide.join("\n").contains("resize line 12"));
}

#[test]
fn automatic_live_preview_fills_available_height_and_yields_to_composer() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("生成长回复".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (1..=40)
            .map(|index| format!("response line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let height = 30;
    let before_draft = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
        &state, 96, height, -1,
    ));
    let before_count = live_box_content_line_count(&before_draft, "Working · Streaming response");
    assert!(before_count > 15, "auto 模式应超过旧默认上限");

    state.push_input_text(&(0..20).map(|_| "draft\n").collect::<String>());
    let with_draft = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
        &state, 96, height, -1,
    ));
    let with_draft_count = live_box_content_line_count(&with_draft, "Working · Streaming response");

    assert!(
        with_draft_count < before_count,
        "composer 扩高时框应让出空间"
    );
    assert!(with_draft.len() <= usize::from(height));
    assert!(with_draft.join("\n").contains("draft"));
    assert!(with_draft.join("\n").contains("response line 40"));
}

#[test]
fn completed_assistant_stays_visible_until_pending_turn_commits() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("你好".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "你好，我在。".into(),
    });

    let live_text = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let scrollback_before_commit = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback_before_commit.contains("› 你好"));
    assert!(live_text.contains("你好，我在。"));
    assert!(!scrollback_before_commit.contains("你好，我在。"));

    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    let scrollback_after_commit = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback_after_commit.contains("› 你好"));
    assert!(scrollback_after_commit.contains("你好，我在。"));
}

#[test]
fn pre_tool_assistant_text_stays_visible_while_tool_runs() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查一下".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "我先查路由。".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "我先查路由。".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_router".into(),
        name: "consult_router".into(),
        summary: "tool consult_router {\"query\":\"查一下\"}".into(),
    });

    let during_tool = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let scrollback = state
        .scrollback_lines(96)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback.contains("› 查一下"));
    assert!(!during_tool.contains("› 查一下"));
    assert!(during_tool.contains("我先查路由。"));
    assert!(during_tool.contains("Calling consult_router"));

    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_router".into(),
        summary: "tool consult_router ok claims=1 disputes=0".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "查到了。".into(),
    });
    let after_tool = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(after_tool.contains("我先查路由。"));
    assert!(after_tool.contains("Called consult_router"));
    assert!(after_tool.contains("查到了。"));
    let first_assistant = after_tool.find("我先查路由。").unwrap();
    let completed_tool = after_tool.find("Called consult_router").unwrap();
    let second_assistant = after_tool.find("查到了。").unwrap();
    assert!(
        first_assistant < completed_tool && completed_tool < second_assistant,
        "Active turn live region should preserve assistant/tool order:\n{after_tool}"
    );
}

#[test]
fn running_tool_is_separated_from_preceding_streamed_text() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查 LA 高薪工作".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "我先抓取官方薪资数据。".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "我先抓取官方薪资数据。".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_code".into(),
        name: "code_run".into(),
        summary: r#"tool code_run {"script":"python3 - <<'PY'\nprint('hi')\nPY"}"#.into(),
    });

    let lines = lines_plain_text(&super::inline_live_lines_with_width(&state, 96));
    let assistant_index = lines
        .iter()
        .position(|line| line.contains("我先抓取官方薪资数据。"))
        .expect("Assistant text should render");
    let tool_index = lines
        .iter()
        .position(|line| line.contains("Calling code_run"))
        .expect("Running tool should render");

    assert_eq!(
        tool_index,
        assistant_index + 2,
        "Streamed text and running tool should be separated:\n{}",
        lines.join("\n")
    );
}

#[test]
fn completed_tool_is_separated_from_following_running_tool() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查 LA 高薪工作".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "我先抓取官方薪资数据。".into(),
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "我先抓取官方薪资数据。".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_code".into(),
        name: "code_run".into(),
        summary: r#"tool code_run {"script":"python3 - <<'PY'\nprint('hi')\nPY"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_code".into(),
        summary: "tool code_run ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::ProcessExit {
            exit_code: Some(0),
            success: true,
        },
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_web".into(),
        name: "web_search".into(),
        summary: r#"tool web_search {"query":"Los Angeles jobs salary 150000"}"#.into(),
    });

    let lines = lines_plain_text(&super::inline_live_lines_with_width(&state, 96));
    let input_detail_index = lines
        .iter()
        .position(|line| line.contains("python3"))
        .expect("Completed tool detail should render");
    let outcome_detail_index = lines
        .iter()
        .position(|line| line.contains("Process exit code: 0"))
        .expect("Typed process outcome should render");
    let running_tool_index = lines
        .iter()
        .position(|line| line.contains("Calling web_search"))
        .expect("Following running tool should render");

    assert_eq!(
        running_tool_index,
        outcome_detail_index + 2,
        "Completed tool and following running tool should be separated:\n{}",
        lines.join("\n")
    );
    assert_eq!(outcome_detail_index, input_detail_index + 1);
}

#[test]
fn parallel_tool_cells_keep_source_order_while_completing_out_of_order() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("并行读取三个文件".into());
    for (id, path) in [
        ("toolu_a", "a.txt"),
        ("toolu_b", "b.txt"),
        ("toolu_c", "c.txt"),
    ] {
        state.apply_event(SessionEvent::ToolCallStarted {
            id: id.into(),
            name: "file_read".into(),
            summary: format!(r#"tool file_read {{"path":"{path}"}}"#),
        });
    }
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_b".into(),
        summary: "tool file_read ok b".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let during = lines_plain_text(&super::inline_live_lines_with_width(&state, 96)).join("\n");
    let a = during
        .find("a.txt")
        .expect("First source call should render");
    let b = during
        .find("b.txt")
        .expect("Second source call should render");
    let c = during
        .find("c.txt")
        .expect("Third source call should render");
    assert!(
        a < b && b < c,
        "ToolCells must keep source order:\n{during}"
    );
    assert_eq!(during.matches("Calling file_read").count(), 2);
    assert_eq!(during.matches("Called file_read").count(), 1);

    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_c".into(),
        summary: "tool file_read ok c".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_a".into(),
        summary: "tool file_read ok a".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    let settled = lines_plain_text(&super::inline_live_lines_with_width(&state, 96)).join("\n");
    let a = settled
        .find("a.txt")
        .expect("First source call should remain");
    let b = settled
        .find("b.txt")
        .expect("Second source call should remain");
    let c = settled
        .find("c.txt")
        .expect("Third source call should remain");
    assert!(
        a < b && b < c,
        "Completion must update in place:\n{settled}"
    );
    assert_eq!(settled.matches("Called file_read").count(), 3);
    assert!(!settled.contains("Calling file_read"));
}

#[test]
fn cancelled_parallel_tools_close_started_cells_and_show_queued_call_as_skipped() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("取消并行工具".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_a".into(),
        name: "web_fetch".into(),
        summary: r#"tool web_fetch {"url":"https://example.com/a"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_b".into(),
        name: "web_fetch".into(),
        summary: r#"tool web_fetch {"url":"https://example.com/b"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallSkipped {
        id: "toolu_c".into(),
        name: "web_fetch".into(),
        summary: r#"tool web_fetch {"url":"https://example.com/c"}"#.into(),
        reason: ToolCallSkipReason::TurnCancelledBeforeDispatch,
    });
    state.apply_event(SessionEvent::ToolCallInterrupted {
        id: "toolu_b".into(),
        summary: "tool web_fetch interrupted".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_a".into(),
        summary: "tool web_fetch http_status=200".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::HttpResponse { http_status: 200 },
    });

    let live = lines_plain_text(&state.active_assistant_lines(96)).join("\n");
    let a = live
        .find("example.com/a")
        .expect("Completed call should remain");
    let b = live
        .find("example.com/b")
        .expect("Interrupted call should remain");
    let c = live
        .find("example.com/c")
        .expect("Skipped call should render");
    assert!(
        a < b && b < c,
        "Terminal cells must keep source order:\n{live}"
    );
    assert!(live.contains("Called web_fetch"));
    assert!(live.contains("Interrupted web_fetch"));
    assert!(live.contains("Skipped web_fetch"));
    assert!(live.contains("Turn cancelled before dispatch"));
    assert!(!live.contains("Calling web_fetch"));
    assert!(!live.contains("elapsed"));

    state.apply_event(SessionEvent::TurnCancelled {
        reason: "user cancelled turn".into(),
    });
    let transcript = state.transcript_text();
    assert!(!transcript.contains("Calling web_fetch"));
}

#[test]
fn mcp_tool_progress_is_visible_while_tool_runs() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("问 pal".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_pal".into(),
        name: "mcp__pal__ask".into(),
        summary: r#"tool mcp__pal__ask {"q":"hi"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallProgress {
        id: "toolu_pal".into(),
        summary: "tool mcp__pal__ask progress 1/2 half".into(),
    });

    let during_tool = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(during_tool.contains("Calling mcp pal/ask"));
    assert!(during_tool.contains("progress 1/2 half"));
    assert!(during_tool.contains("elapsed"));
}

#[test]
fn completed_tool_call_stays_in_live_region_until_turn_commits() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查一下今天美股收盘".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_web".into(),
        name: "web_search".into(),
        summary: "tool web_search {\"query\":\"今日 美股 收盘\"}".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_web".into(),
        summary: "tool web_search ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "我查完了，正在整理。".into(),
    });

    let live_during_answer = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let scrollback_before_commit = state
        .scrollback_lines(96)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(live_during_answer.contains("Called web_search"));
    assert!(live_during_answer.contains("{\"query\":\"今日 美股 收盘\"}"));
    assert!(live_during_answer.contains("我查完了，正在整理。"));
    assert!(!scrollback_before_commit.contains("Called web_search"));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "我查完了，正在整理。".into(),
    });
    let live_after_assistant_complete = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(live_after_assistant_complete.contains("Called web_search"));

    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });
    let live_after_commit = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let scrollback_after_commit = state
        .scrollback_lines(96)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(!live_after_commit.contains("Called web_search"));
    assert!(scrollback_after_commit.contains("Called web_search"));
    assert!(scrollback_after_commit.contains("{\"query\":\"今日 美股 收盘\"}"));
}

#[test]
fn completed_tool_is_separated_from_following_streaming_assistant_text() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查 PDF".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_read".into(),
        name: "file_read".into(),
        summary: r#"tool file_read {"path":"a.pdf"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_read".into(),
        summary: "tool file_read ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "我找到了 10 个 PDF。".into(),
    });

    let live = super::inline_live_lines_with_width(&state, 96);
    let lines = lines_plain_text(&live);
    let detail_index = lines
        .iter()
        .position(|line| line.contains(r#"{"path":"a.pdf"}"#))
        .expect("Tool detail should render");
    let assistant_index = lines
        .iter()
        .position(|line| line.contains("我找到了 10 个 PDF。"))
        .expect("Assistant text should render");

    assert!(
        !lines[detail_index + 1].contains("a.pdf") && !lines[detail_index + 1].contains("我找到了"),
        "Completed tool and following assistant text should be separated:\n{}",
        lines.join("\n")
    );
    assert_eq!(assistant_index, detail_index + 2);
}

#[test]
fn committed_tool_is_separated_from_following_assistant_text_in_scrollback() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("查 PDF".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_read".into(),
        name: "file_read".into(),
        summary: r#"tool file_read {"path":"a.pdf"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_read".into(),
        summary: "tool file_read ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "我找到了 10 个 PDF。".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    let scrollback = super::inline_scrollback_lines_with_width(&state, 96);
    let lines = lines_plain_text(&scrollback);
    let detail_index = lines
        .iter()
        .position(|line| line.contains(r#"{"path":"a.pdf"}"#))
        .expect("Tool detail should render");
    let assistant_index = lines
        .iter()
        .position(|line| line.contains("我找到了 10 个 PDF。"))
        .expect("Assistant text should render");

    assert!(
        lines[detail_index + 1].trim().is_empty(),
        "Committed tool and following assistant text should be separated:\n{}",
        lines.join("\n")
    );
    assert_eq!(assistant_index, detail_index + 2);
}

#[test]
fn pending_turn_feedback_is_visible_before_engine_event() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("你好".into());

    assert!(state.transcript_text().contains("› 你好"));
    assert!(super::history_render_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("thinking")));
}

#[test]
fn pending_turn_cursor_stays_on_composer_input_line() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("你好".into());

    assert!(super::inline_cursor_for_width(&state, 80).is_some());
}

#[test]
fn active_turn_prompt_flushes_to_scrollback_while_streaming_output_stays_live() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("好的，帮我梳理一下支付模块逻辑。".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "分析结论：当前实现整体思路正确".into(),
    });

    let scrollback = state.scrollback_lines(80);
    assert_eq!(scrollback.entry_count, 1);
    let scrollback_text = scrollback
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(scrollback_text.contains("好的，帮我梳理一下支付模块逻辑"));

    let live = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let box_index = live
        .iter()
        .position(|line| line.contains("Working · Streaming response"))
        .expect("Live working box should be visible");
    let answer_index = live
        .iter()
        .position(|line| line.contains("分析结论"))
        .expect("Streaming answer should be visible");

    assert!(box_index < answer_index);
    assert!(!live
        .iter()
        .any(|line| line.contains("好的，帮我梳理一下支付模块逻辑")));
}

#[test]
fn active_live_region_respects_terminal_width() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("abcdefghijklmnopqrstuvwxyz".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "workspace_read".into(),
        summary: format!("tool workspace_read {}", "x".repeat(120)),
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz".into(),
    });

    let width = 24;
    let live = super::inline_live_lines_with_width(&state, width);

    assert!(live.iter().all(|line| line.width() <= usize::from(width)));
}

#[test]
fn streaming_preview_is_capped_inside_live_region() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("生成长回复".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (0..20)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let live_text = super::inline_live_lines_with_size_and_preview_max(&state, 80, u16::MAX, 15)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(live_text.contains("Working · Streaming response"));
    assert!(live_text.contains("line 19"));
    assert!(!live_text.contains("line 0"));
}

#[test]
fn streaming_preview_shrinks_on_short_terminals() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("生成长回复".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (0..12)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let live = lines_plain_text(&super::inline_live_lines_with_size(&state, 72, 12));
    let live_text = live.join("\n");
    let content_lines = live_box_content_line_count(&live, "Working · Streaming response");

    assert!(live_text.contains("line 11"));
    assert_eq!(content_lines, 7);
    assert!(live.len() <= 12);
}

#[test]
fn mixed_tool_and_assistant_rows_share_short_terminal_preview_budget() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("抓取长 URL".into());
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_long".into(),
        name: "web_fetch".into(),
        summary: format!(
            r#"tool web_fetch {{"url":"https://example.com/{}"}}"#,
            "very-long-segment-".repeat(8)
        ),
    });
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (0..7)
            .map(|index| format!("assistant line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let width = 52;
    let height = 18;
    let live = lines_plain_text(&super::inline_live_lines_with_size(&state, width, height));
    let text = live.join("\n");

    assert!(
        live.len() <= usize::from(height),
        "工具与 assistant 内容必须共用终端可用高度预算:\n{text}"
    );
    assert_eq!(
        live_box_content_line_count(&live, "Working · Streaming response"),
        13,
        "18 行终端扣除两行边框、composer 与 footer 后应自适应为 13 行"
    );
    assert!(text.contains("  ..."));
    assert!(text.contains("very-long-segment"));
    assert!(text.contains("assistant line 6"));
    assert!(
        !text.contains("Calling web_fetch"),
        "允许从工具块中间逐行截断，以保持框高稳定"
    );
}

#[test]
fn sequential_tool_cells_keep_live_region_bounded_and_follow_latest_call() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("连续调用 20 个 code_run".into());
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });
    state.apply_event(SessionEvent::LocalClaimsUpdated { total: 9 });

    let width = 150;
    let height = 36;
    for index in 1..=20 {
        let id = format!("toolu_{index}");
        state.apply_event(SessionEvent::ToolCallStarted {
            id: id.clone(),
            name: "code_run".into(),
            summary: format!(r#"tool code_run {{"script":"print('helloworld {index}')"}}"#),
        });

        let running = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
            &state, width, height, 15,
        ));
        assert!(
            running.len() <= usize::from(height),
            "第 {index} 个工具开始后 live region 超过终端高度: {} > {height}\n{}",
            running.len(),
            running.join("\n")
        );
        assert!(
            running.join("\n").contains(&format!("helloworld {index}")),
            "live region 应跟随显示最新工具调用"
        );
        let running_box_rows =
            live_box_content_line_count(&running, "Working · Streaming response");
        assert!(running_box_rows <= 15);
        if index >= 5 {
            assert_eq!(
                running_box_rows,
                15,
                "溢出后逐行滑动时框内高度不应因工具块边界而跳变:\n{}",
                running.join("\n")
            );
        }
        assert!(running.join("\n").contains("local claims 9"));

        state.apply_event(SessionEvent::ToolCallCompleted {
            id,
            summary: format!("tool code_run exit_code=0 helloworld {index}"),
            file_change: None,
            outcome: ToolExecutionOutcome::ProcessExit {
                exit_code: Some(0),
                success: true,
            },
        });

        let completed = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
            &state, width, height, 15,
        ));
        assert!(
            completed.len() <= usize::from(height),
            "第 {index} 个工具完成后 live region 超过终端高度: {} > {height}\n{}",
            completed.len(),
            completed.join("\n")
        );
        assert!(
            completed
                .join("\n")
                .contains(&format!("helloworld {index}")),
            "live region 应跟随显示最新工具结果"
        );
        let completed_box_rows =
            live_box_content_line_count(&completed, "Working · Streaming response");
        assert!(completed_box_rows <= 15);
        if index >= 5 {
            assert_eq!(
                completed_box_rows, 15,
                "工具完成后仍应维持统一的 15 行框内预算"
            );
        }
        assert!(completed.join("\n").contains("local claims 9"));
    }

    let cropped = lines_plain_text(&super::inline_live_lines_with_size_and_preview_max(
        &state, width, height, 15,
    ));
    let cropped_text = cropped.join("\n");
    assert!(cropped_text.contains("  ..."));
    assert!(!cropped_text.contains("helloworld 1')"));
    assert!(cropped_text.contains("helloworld 20"));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "20 个工具全部完成".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 4 });
    let committed = lines_plain_text(&state.scrollback_lines(width).lines).join("\n");
    assert!(committed.contains("helloworld 1"));
    assert!(committed.contains("helloworld 20"));
    assert!(committed.contains("20 个工具全部完成"));

    state.reset_flushed_for_hard_clear();
    let after_resize = lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 90));
    let after_resize_text = after_resize.join("\n");
    assert!(after_resize_text.contains("helloworld 1"));
    assert!(after_resize_text.contains("helloworld 20"));
    assert!(after_resize_text.contains("20 个工具全部完成"));
}

#[test]
fn long_active_user_prompt_flushes_fully_so_live_box_stays_visible() {
    let mut state = super::TuiState::new();
    let prompt = (1..=30)
        .map(|index| format!("prompt line {index}: {}", "x".repeat(80)))
        .collect::<Vec<_>>()
        .join("\n");
    state.begin_pending_turn(prompt);
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "streaming answer".into(),
    });

    let scrollback_text = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let height: u16 = 18;
    let lines = lines_plain_text(&super::inline_live_lines_with_size(&state, 80, height));
    let text = lines.join("\n");
    let box_top = lines
        .iter()
        .position(|line| line.contains("Working · Streaming response"))
        .expect("Working box top should stay visible");
    let box_bottom = lines
        .iter()
        .position(|line| line.starts_with('└'))
        .expect("Working box bottom should stay visible");

    assert!(
        lines.len() <= usize::from(height),
        "Live region should fit the terminal height:\n{text}"
    );
    assert!(scrollback_text.contains("prompt line 1"));
    assert!(scrollback_text.contains("prompt line 30"));
    assert!(!scrollback_text.contains("omitted"));
    assert!(!text.contains("prompt line 1"));
    assert!(!text.contains("omitted"));
    assert!(text.contains("streaming answer"));
    assert!(box_top < box_bottom);
}

#[test]
fn streaming_preview_single_line_budget_keeps_latest_line_only() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("生成长回复".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: (0..12)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n"),
    });

    let live_text = super::inline_live_lines_with_size(&state, 72, 1)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(live_text.contains("line 11"));
    assert!(!live_text.contains("  ..."));
    assert!(!live_text.contains("line 10"));
}

#[test]
fn tool_completion_keeps_thinking_visible_while_turn_is_running() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Running,
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_1".into(),
        name: "memory".into(),
        summary: "tool memory {\"action\":\"add\"}".into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_1".into(),
        summary: "tool memory ok".into(),
        file_change: None,
        outcome: ToolExecutionOutcome::Completed,
    });

    assert!(super::history_render_lines_with_width(&state, 80)
        .iter()
        .any(|line| line.to_string().contains("thinking")));
}

#[test]
fn running_turn_accepts_and_queues_next_user_input() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());

    assert!(super::input_accepts_text(SessionRuntimeStatus::Running));
    assert!(state.has_turn_in_flight());
    state.queue_pending_turn("第二条");
    assert_eq!(state.queued_count(), 1);
    assert!(!state.transcript_text().contains("› 第二条"));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    let queued = state.pop_queued_turn().unwrap();
    state.begin_pending_turn(queued.text().to_string());
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "第二条".into(),
    });
    assert_eq!(state.transcript_text().matches("› 第二条").count(), 1);
    let transcript = state.transcript_text();
    let first_user = transcript.find("› 第一条").unwrap();
    let first_assistant = transcript.find("第一条回复").unwrap();
    let second_user = transcript.find("› 第二条").unwrap();
    assert!(first_user < first_assistant);
    assert!(first_assistant < second_user);
}

#[test]
fn queued_input_is_visible_as_pending_preview_when_draft_is_empty() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");

    assert_eq!(state.input(), "");
    let rendered = super::composer_lines_with_width(&state, 16)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert!(rendered[0].contains("›"));
    assert!(rendered[1].contains("queued: 第二条"));
}

#[test]
fn status_notice_renders_between_live_box_and_composer_without_command_echo() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("first prompt".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "streaming answer".into(),
    });
    state.push_input_text("next draft");
    state.set_status_notice("MCP panel is available when the current turn is idle.");

    let lines = lines_plain_text(&super::inline_live_lines_with_width(&state, 96));
    let live_text = lines.join("\n");
    let scrollback_text = super::inline_scrollback_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let box_bottom_index = lines
        .iter()
        .position(|line| line.starts_with('└'))
        .expect("Live box bottom should render");
    let notice_index = lines
        .iter()
        .position(|line| line.contains("MCP panel is available when the current turn is idle."))
        .expect("Status notice should render");
    let composer_index = lines
        .iter()
        .position(|line| line.contains("next draft"))
        .expect("Composer draft should render after the notice");

    assert!(live_text.contains("MCP panel is available when the current turn is idle."));
    assert!(!lines[notice_index].starts_with('┆'));
    assert!(box_bottom_index < notice_index);
    assert!(notice_index < composer_index);
    assert!(!scrollback_text.contains("MCP panel is available when the current turn is idle."));
}

#[test]
fn pending_tool_boundary_steer_renders_between_live_box_and_composer() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("上一轮请求".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "工具还在执行".into(),
    });
    state.set_pending_tool_boundary_steer(Some("为什么会 404，你再尝试下".into()));
    state.push_input_text("新的草稿");

    let lines = super::inline_live_lines_with_width(&state, 96);
    let plain = lines_plain_text(&lines);
    let box_bottom_index = plain
        .iter()
        .position(|line| line.starts_with('└'))
        .expect("Live box bottom should render");
    let steer_index = plain
        .iter()
        .position(|line| line.contains("为什么会 404，你再尝试下"))
        .expect("Pending steer should render");
    let composer_index = plain
        .iter()
        .position(|line| line.contains("新的草稿"))
        .expect("Composer should render after pending steer");
    let scrollback = state
        .scrollback_lines(96)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback.contains("上一轮请求"));
    assert!(!plain.iter().any(|line| line.contains("上一轮请求")));
    assert_eq!(steer_index, box_bottom_index + 2);
    assert!(plain[box_bottom_index + 1].is_empty());
    assert!(plain[steer_index].starts_with("› "));
    assert!(plain[steer_index + 1].is_empty());
    assert_eq!(composer_index, steer_index + 2);
    assert_eq!(lines[steer_index].style.fg, Some(Color::Black));
    assert_eq!(lines[steer_index].style.bg, Some(Color::Gray));
}

#[test]
fn running_turn_without_pending_steer_does_not_gap_before_composer() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("你是谁".into());

    let lines = lines_plain_text(&super::inline_live_lines_with_width(&state, 96));
    let box_bottom_index = lines
        .iter()
        .position(|line| line.starts_with('└'))
        .expect("Live box bottom should render");
    let composer_index = lines
        .iter()
        .position(|line| line.contains("Whisper your wish here"))
        .expect("Empty composer should render after live box");

    assert_eq!(composer_index, box_bottom_index + 1);
}

#[test]
fn pending_tool_boundary_steer_clears_when_turn_finishes() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("上一轮请求".into());
    state.set_pending_tool_boundary_steer(Some("继续".into()));
    state.set_status_notice("Turn cancel pending: waiting for current turn boundary");

    assert!(!state.pending_tool_boundary_steer_lines(80).is_empty());

    state.mark_turn_finished();

    assert!(state.pending_tool_boundary_steer_lines(80).is_empty());
    assert!(state.status_notice_text().is_none());
}

#[test]
fn status_notice_clears_when_session_returns_idle() {
    let mut state = super::TuiState::new();
    state.set_status_notice("MCP panel is available when the current turn is idle.");

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    let live_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!live_text.contains("MCP panel is available when the current turn is idle."));
}

#[test]
fn delegation_status_notice_clears_when_session_returns_idle() {
    let mut state = super::TuiState::new();
    state.set_status_notice("Subagent scan completed");

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    let live_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!live_text.contains("Subagent scan completed"));
}

#[test]
fn delegation_status_notice_survives_idle_while_session_has_subagents() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![delegation_summary(
        "subagent_11111111",
        "scan",
        "researcher",
        DelegationStatus::Completed,
        Some("done"),
    )]);
    state.set_status_notice("Subagent scan completed");

    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));
    assert!(live_text.contains("Subagent scan completed"));
}

#[test]
fn new_turn_clears_delegation_status_notice_when_all_subagents_terminal() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![delegation_summary(
        "subagent_11111111",
        "scan",
        "researcher",
        DelegationStatus::Completed,
        Some("done"),
    )]);
    state.set_status_notice("Subagent scan completed");

    state.begin_pending_turn("继续".into());

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));
    assert!(!live_text.contains("Subagent scan completed"));
    assert!(live_text.contains("Subagents: 1 completed · /subagents"));
}

#[test]
fn new_turn_keeps_delegation_status_notice_while_subagents_are_unfinished() {
    let mut state = super::TuiState::new();
    state.set_delegation_summaries(vec![
        delegation_summary(
            "subagent_11111111",
            "done",
            "researcher",
            DelegationStatus::Completed,
            Some("done"),
        ),
        delegation_summary(
            "subagent_22222222",
            "running",
            "researcher",
            DelegationStatus::Running,
            Some("reading"),
        ),
    ]);
    state.set_status_notice("Subagent done completed");

    state.begin_pending_turn("继续".into());

    let live_text = lines_text(&super::inline_live_lines_with_width(&state, 160));
    assert!(live_text.contains("Subagent done completed"));
    assert!(live_text
        .contains("Subagents: 1 completed · 1 running · Subagent done completed · /subagents"));
}

#[test]
fn queued_preview_stays_visible_below_active_user_prompt() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("first prompt".into());
    state.queue_pending_turn("second prompt");

    let lines = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let queued_index = lines
        .iter()
        .position(|line| line.contains("queued: second prompt"))
        .expect("Queued preview should stay visible");
    let box_index = lines
        .iter()
        .position(|line| line.contains("Working · Streaming response"))
        .expect("Running live box should be visible");
    let scrollback = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback.contains("first prompt"));
    assert!(!lines.iter().any(|line| line.contains("first prompt")));
    assert!(box_index < queued_index);
}

#[test]
fn running_draft_uses_new_composer_below_streaming_box() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("first prompt".into());
    state.apply_event(SessionEvent::AssistantTextDelta {
        text: "streaming answer".into(),
    });
    state.push_input_text("second draft");

    let lines = super::inline_live_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    let box_index = lines
        .iter()
        .position(|line| line.contains("Working · Streaming response"))
        .expect("Running live box should be visible");
    let draft_index = lines
        .iter()
        .position(|line| line.contains("second draft"))
        .expect("New draft should be visible");
    let scrollback = state
        .scrollback_lines(80)
        .lines
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(scrollback.contains("first prompt"));
    assert!(!lines.iter().any(|line| line.contains("first prompt")));
    assert!(box_index < draft_index);
}

#[test]
fn queued_preview_truncates_long_inputs() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn(format!("//! {}\nfn main() {{}}", "x".repeat(300)));

    let rendered = super::composer_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(rendered.contains("queued: //!"));
    assert!(rendered.contains("..."));
    assert!(!rendered.contains("fn main"));
}

#[test]
fn open_status_before_worker_finish_still_queues_input() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::StatusChanged {
        status: SessionRuntimeStatus::Open,
    });

    assert!(state.has_turn_in_flight());
    state.queue_pending_turn("第二条");
    assert_eq!(state.queued_count(), 1);
    state.mark_turn_finished();
    assert!(!state.has_turn_in_flight());
}

#[test]
fn committed_turn_clears_tui_in_flight_hint_before_worker_finish() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.begin_pending_turn("第一条".into());
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "第一条回复".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    assert!(!state.has_turn_in_flight());
    let live_text = super::inline_live_lines_with_width(&state, 96)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!live_text.contains("turn committing"));
    assert!(live_text.contains("Enter sends"));
}

#[test]
fn cancelling_running_turn_keeps_queue_but_does_not_start_it() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");
    state.queue_pending_turn("第三条");

    state.cancel_running_turn("user pressed esc");

    assert_eq!(state.status, SessionRuntimeStatus::Open);
    assert!(!state.has_turn_in_flight());
    assert_eq!(state.queued_count(), 0);
    assert_eq!(state.input(), "第二条\n第三条");
    assert!(!state.transcript_text().contains("› 第二条"));
    assert!(!state.transcript_text().contains("› 第三条"));
    assert!(state.transcript_text().contains("Turn cancelled"));
}

#[tokio::test]
async fn committed_turn_is_not_cancellable_while_worker_finishes() {
    let handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
    let mut session_task = super::SessionTaskState::default();
    session_task.current = Some(super::ActiveSessionTask::Turn(Box::new(
        super::ActiveTurn::new_for_test(7, handle),
    )));

    session_task.mark_turn_committed(7);

    assert!(!session_task.request_tool_boundary_cancel("late cancel"));
    assert!(
        !session_task
            .request_tool_boundary_steer(
                9,
                &super::input_queue::QueuedInput::from_text("late steer")
            )
            .await
    );

    if let Some(super::ActiveSessionTask::Turn(active)) = session_task.current.take() {
        active.handle.abort();
    }
}

#[test]
fn restored_queued_input_is_echoed_when_sent_after_cancel() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");
    state.queue_pending_turn("第三条");
    state.cancel_running_turn("user pressed esc");

    let restored = state.take_input();
    assert_eq!(restored, "第二条\n第三条");
    state.begin_pending_turn(restored);

    let transcript = state.transcript_text();
    assert_eq!(transcript.matches("› 第二条").count(), 1);
    assert_eq!(transcript.matches("  第三条").count(), 1);
    assert_eq!(state.queued_count(), 0);
    assert_eq!(state.input(), "");
}

#[test]
fn cancelling_running_turn_restores_all_queued_inputs_as_multiline_draft() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");
    state.queue_pending_turn("第三条");

    state.cancel_running_turn("user pressed esc");

    assert_eq!(state.queued_count(), 0);
    assert_eq!(state.input(), "第二条\n第三条");
}

#[test]
fn cancelling_running_turn_merges_queue_before_existing_draft() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");
    state.queue_pending_turn("第三条");
    state.push_input_char('草');
    state.push_input_char('稿');

    state.cancel_running_turn("user pressed esc");

    assert_eq!(state.queued_count(), 0);
    assert_eq!(state.input(), "第二条\n第三条\n草稿");
    assert_eq!(super::composer_height(&state), 4);
    let rendered = super::composer_lines_with_width(&state, 16)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert!(rendered[0].contains("第二条"));
    assert!(rendered[1].contains("第三条"));
    assert!(rendered[2].contains("草稿"));
}

#[test]
fn restoring_ready_async_inputs_preserves_sequence_before_current_draft() {
    let mut state = super::TuiState::new();
    state.push_input_text("当前草稿");
    state.restore_input_drafts_preserving_current(vec![
        super::input_queue::QueuedInput::from_text("附件输入 A").into_draft(),
        super::input_queue::QueuedInput::from_text("普通输入 B").into_draft(),
    ]);

    assert_eq!(state.input(), "附件输入 A\n普通输入 B\n当前草稿");
}

#[test]
fn resume_interruption_restores_queued_inputs_before_current_draft() {
    let mut state = super::TuiState::new();
    state.queue_pending_turn("resume 期间输入的第一条");
    state.queue_pending_turn("resume 期间输入的第二条");
    state.push_input_text("未发送草稿");

    state.restore_queued_inputs_to_composer();

    assert_eq!(state.queued_count(), 0);
    assert_eq!(
        state.input(),
        "resume 期间输入的第一条\nresume 期间输入的第二条\n未发送草稿"
    );
}

#[test]
fn cancelling_running_turn_restores_queued_large_paste_as_placeholder() {
    let mut state = super::TuiState::new();
    let pasted = format!("//! {}\nfn main() {{}}", "x".repeat(1200));
    state.begin_pending_turn("第一条".into());
    state.push_pasted_text(&pasted);
    let queued = state.take_input_draft();
    state.queue_pending_turn(super::input_queue::QueuedInput::new(pasted.clone(), queued));

    state.cancel_running_turn("user pressed esc");

    assert!(state.input().starts_with("[Pasted Content "));
    assert!(!state.input().contains("fn main"));
    assert_eq!(state.take_input(), pasted);
}

#[test]
fn restoring_multiple_same_size_large_pastes_preserves_expanded_text() {
    let mut state = super::TuiState::new();
    let first = format!("first {}", "x".repeat(1200));
    let second = format!("second {}", "y".repeat(1199));
    let current = format!("current {}", "z".repeat(1198));
    state.begin_pending_turn("第一条".into());
    state.push_pasted_text(&first);
    state.push_pasted_text(&second);
    let queued = state.take_input_draft();
    state.queue_pending_turn(super::input_queue::QueuedInput::new(
        format!("{first}{second}"),
        queued,
    ));
    state.push_pasted_text(&current);

    state.cancel_running_turn("user pressed esc");

    let restored_visible = state.input().to_string();
    assert!(restored_visible.contains("#2]"));
    assert!(restored_visible.contains("#3]"));
    assert_eq!(state.take_input(), format!("{first}{second}\n{current}"));
}

#[test]
fn restoring_large_pastes_across_queued_drafts_avoids_placeholder_collisions() {
    let mut state = super::TuiState::new();
    let first = format!("first {}", "x".repeat(1200));
    let second = format!("second {}", "y".repeat(1199));
    let third = format!("third {}", "z".repeat(1200));
    state.begin_pending_turn("第一条".into());
    state.push_pasted_text(&first);
    let first_queued = state.take_input_draft();
    state.queue_pending_turn(super::input_queue::QueuedInput::new(
        first.clone(),
        first_queued,
    ));
    state.push_pasted_text(&second);
    state.push_pasted_text(&third);
    let second_queued = state.take_input_draft();
    state.queue_pending_turn(super::input_queue::QueuedInput::new(
        format!("{second}{third}"),
        second_queued,
    ));

    state.cancel_running_turn("user pressed esc");

    let restored_visible = state.input().to_string();
    assert!(restored_visible.contains("#2]"));
    assert!(restored_visible.contains("#3]"));
    assert_eq!(state.take_input(), format!("{first}\n{second}{third}"));
}

#[test]
fn large_paste_expansion_does_not_replace_placeholder_text_inside_pasted_body() {
    let mut state = super::TuiState::new();
    let first = format!(
        "contains [Pasted Content 1206 chars #2] {}",
        "x".repeat(1160)
    );
    let second = format!("second {}", "y".repeat(1199));
    state.push_pasted_text(&first);
    state.push_pasted_text(&second);

    assert_eq!(state.take_input(), format!("{first}{second}"));
}

#[test]
fn failed_running_turn_restores_queued_inputs_to_composer() {
    let mut state = super::TuiState::new();
    state.begin_pending_turn("第一条".into());
    state.queue_pending_turn("第二条");
    state.queue_pending_turn("第三条");

    state.fail_running_turn("network down");

    assert_eq!(state.status, SessionRuntimeStatus::Error);
    assert!(!state.has_turn_in_flight());
    assert_eq!(state.queued_count(), 0);
    assert_eq!(state.input(), "第二条\n第三条");
    assert!(state
        .transcript_text()
        .contains("Turn failed: network down"));
}

#[test]
fn turn_failure_without_output_keeps_one_gap_after_flushed_user_input() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();
    state.begin_pending_turn("北京天气怎么样".into());

    let first_flush = state.scrollback_lines(96);
    let first_render = lines_plain_text(&first_flush.lines);
    state.mark_scrollback_flushed(first_flush.entry_count);
    assert_eq!(first_render.last().map(String::as_str), Some(""));

    state.fail_running_turn("LLM provider returned HTTP 429");
    let mut rendered = first_render;
    rendered.extend(lines_plain_text(&state.scrollback_lines(96).lines));

    let user_index = rendered
        .iter()
        .position(|line| line.contains("› 北京天气怎么样"))
        .expect("失败 turn 的用户输入应在历史区显示");
    let error_index = rendered
        .iter()
        .position(|line| line.contains("Error Turn failed: LLM provider returned HTTP 429"))
        .expect("Turn failed 错误应在历史区显示");
    assert_eq!(
        error_index,
        user_index.saturating_add(2),
        "无 Assistant/工具输出的 Turn failed 应与用户输入恰好间隔一行：\n{}",
        rendered.join("\n")
    );
    assert!(rendered[user_index.saturating_add(1)].is_empty());

    let reflowed = lines_plain_text(&super::history_render_lines_with_width(&state, 96));
    let reflowed_user = reflowed
        .iter()
        .position(|line| line.contains("› 北京天气怎么样"))
        .expect("重排后仍应包含用户输入");
    let reflowed_error = reflowed
        .iter()
        .position(|line| line.contains("Error Turn failed: LLM provider returned HTTP 429"))
        .expect("重排后仍应包含 Turn failed");
    assert_eq!(reflowed_error, reflowed_user.saturating_add(2));
    assert!(reflowed[reflowed_user.saturating_add(1)].is_empty());
}

#[test]
fn assistant_markdown_renders_ordered_lists_tables_and_quotes() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "1. 第一\n2. 第二\n\n| 列A | 列B |\n| --- | --- |\n| x | y |\n\n> 引用".into(),
    });

    let rendered = super::history_render_lines_with_width(&state, 80)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("1. 第一"));
    assert!(rendered.contains("2. 第二"));
    assert!(rendered.contains("列A"));
    assert!(rendered.contains("列B"));
    assert!(rendered.contains("> 引用"));
}

#[test]
fn file_diff_renders_in_scrollback_only_after_turn_commit() {
    let mut state = super::TuiState::new();
    state.apply_event(SessionEvent::SessionStarted {
        session_id: SessionId::from_str("session_1234abcd").unwrap(),
        agent_id: AgentId::new("agent-a").unwrap(),
    });
    state.mark_start_separator_flushed();
    state.begin_pending_turn("改文件".into());
    state.apply_event(SessionEvent::UserMessageAccepted {
        text: "改文件".into(),
    });
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_fp".into(),
        name: "file_patch".into(),
        summary: r#"tool file_patch {"path":"note.txt"}"#.into(),
    });
    let change = crate::tool::diff::compute_file_change(
        "note.txt",
        crate::tool::diff::FileChangeKind::Modified,
        "old\n",
        "new\n",
        20,
    )
    .expect("需产出 diff");
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_fp".into(),
        summary: "tool file_patch ok".into(),
        file_change: Some(change),
        outcome: ToolExecutionOutcome::Completed,
    });

    // turn 未提交：diff 不出现在 live 虚线框，也未落 scrollback。
    let live_running =
        lines_plain_text(&super::inline_live_lines_with_width(&state, 100)).join("\n");
    assert!(live_running.contains("Called file_patch"));
    assert!(!live_running.contains("(+1 -1)"));
    let scrollback_running =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 100)).join("\n");
    assert!(!scrollback_running.contains("(+1 -1)"));

    state.apply_event(SessionEvent::AssistantMessageCompleted {
        text: "改完了".into(),
    });
    state.apply_event(SessionEvent::TurnCommitted { message_count: 2 });

    let scrollback =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 100)).join("\n");
    assert!(
        scrollback.contains("note.txt (+1 -1)"),
        "scrollback 应有 diff 头: {scrollback}"
    );
    assert!(scrollback.contains("- old"));
    assert!(scrollback.contains("+ new"));
    let live_after = lines_plain_text(&super::inline_live_lines_with_width(&state, 100)).join("\n");
    assert!(!live_after.contains("(+1 -1)"));
}

#[test]
fn file_diff_truncation_notice_renders() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();
    let before: String = (1..=30).map(|n| format!("l{n}\n")).collect();
    let after: String = (1..=30).map(|n| format!("L{n}\n")).collect();
    let change = crate::tool::diff::compute_file_change(
        "big.txt",
        crate::tool::diff::FileChangeKind::Modified,
        &before,
        &after,
        5,
    )
    .expect("需产出 diff");
    let truncated = change.truncated_changed_lines;
    assert!(truncated > 0);
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_fw".into(),
        name: "file_write".into(),
        summary: r#"tool file_write {"path":"big.txt"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_fw".into(),
        summary: "tool file_write ok".into(),
        file_change: Some(change),
        outcome: ToolExecutionOutcome::Completed,
    });

    let scrollback =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 100)).join("\n");
    assert!(scrollback.contains(&format!("其余 {truncated} 行改动未展示")));
}

#[test]
fn failed_file_tool_does_not_render_diff() {
    let mut state = super::TuiState::new();
    state.mark_start_separator_flushed();
    let change = crate::tool::diff::compute_file_change(
        "note.txt",
        crate::tool::diff::FileChangeKind::Modified,
        "old\n",
        "new\n",
        20,
    )
    .expect("需产出 diff");
    state.apply_event(SessionEvent::ToolCallStarted {
        id: "toolu_fp".into(),
        name: "file_patch".into(),
        summary: r#"tool file_patch {"path":"note.txt"}"#.into(),
    });
    state.apply_event(SessionEvent::ToolCallCompleted {
        id: "toolu_fp".into(),
        summary: "tool file_patch failed boom".into(),
        file_change: Some(change),
        outcome: ToolExecutionOutcome::BusinessFailure,
    });

    let scrollback =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 100)).join("\n");
    assert!(scrollback.contains("Error: boom"));
    assert!(!scrollback.contains("(+1 -1)"));
}

#[test]
fn resumed_journal_timeline_renders_file_diff() {
    let mut state = super::TuiState::new();
    let change = crate::tool::diff::compute_file_change(
        "note.txt",
        crate::tool::diff::FileChangeKind::Modified,
        "old\n",
        "new\n",
        20,
    )
    .expect("需产出 diff");
    state.push_historical_timeline_turns(&[HistoricalTimelineTurn {
        user_text: "改文件".into(),
        canonical_user_content_hash: None,
        assistant_text: Some("改完了".into()),
        assistant_completed: true,
        status: Some(TurnJournalStatus::Committed),
        tool_calls: vec![TurnJournalToolCall {
            tool_use_id: "toolu_1".into(),
            name: "file_patch".into(),
            started_summary: r#"tool file_patch {"path":"note.txt"}"#.into(),
            input_preview: r#"{"path":"note.txt"}"#.into(),
            input_truncated: false,
            latest_progress: None,
            completed_summary: Some("tool file_patch ok".into()),
            interrupted_summary: None,
            skipped_summary: None,
            skip_reason: None,
            outcome: Some(ToolExecutionOutcome::Completed),
            output_preview: Some(r#"{"ok":true}"#.into()),
            output_truncated: false,
            file_change: Some(change),
        }],
        timeline_items: Vec::new(),
        user_steers: Vec::new(),
        recovery_notice: None,
        turn_status_detail: None,
    }]);

    let scrollback =
        lines_plain_text(&super::inline_scrollback_lines_with_width(&state, 100)).join("\n");
    assert!(
        scrollback.contains("note.txt (+1 -1)"),
        "resume 后应渲染 diff: {scrollback}"
    );
    assert!(scrollback.contains("- old"));
    assert!(scrollback.contains("+ new"));
}
