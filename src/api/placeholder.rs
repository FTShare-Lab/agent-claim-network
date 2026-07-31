//! LLM 一次响应内的占位符替换。
//!
//! ## 背景
//! agent 与 LLM 交互时，LLM 不能"知道"将来要落库的真实 id（claim/dispute），
//! 所以约定它在一次 JSON 响应里：
//! - `new_claims[N].id` 用 `$new_claim_N$`（N 从 0 起递增，可超 1 位）
//! - `new_disputes[N].id` 用 `$new_dispute_N$`
//! - 其它字段（包括 `source_claim_ids`、`new_disputes[*].claims`、自然语言里的
//!   `summary` / `evidence_summary` 等）想引用本次新生成的对象时，写同样的占位符。
//!
//! ## 替换规则
//! 1. 解析 `new_claims` 拿到 `(占位符 → 真实 ClaimId)` 映射；ClaimId 用
//!    `ClaimId::from_claim_parts(now, name, scope)` 派生，与 runner 写盘时一致。
//! 2. 把映射全树广播替换（任意字符串字段里出现都替换，含自然语言）。
//! 3. 同样流程处理 `new_disputes`；此时 `claims` 数组里的 claim 占位符已经被 phase 1
//!    替换为真实 id，可以直接拿去派生 DisputeId。
//! 4. 最终扫一遍整棵 Value，发现仍残留 `$new_claim_N$` / `$new_dispute_N$` 形态的
//!    子串就报"未声明的引用"——这是 LLM 笔误（引用了不存在的占位符）的兜底。
//!
//! ## 顺序与冲突
//! - 占位符末尾固定带 `$`，所以 `$new_claim_1$` 不会是 `$new_claim_11$` 的子串，
//!   广播替换不需要按序号倒序也能正确——映射用任意 Map 即可。
//! - claim 必须先于 dispute 替换：DisputeId 派生输入 `&[ClaimId]`，phase 1 不完成
//!   dispute 的 claim 引用就还是占位符字符串，无法被解析。

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::claim::{ClaimId, DisputeId};

#[derive(Debug, thiserror::Error)]
pub enum PlaceholderError {
    #[error("new_claims[{idx}] 缺少 id 字段或 id 非字符串")]
    ClaimIdMissing { idx: usize },
    #[error("new_claims[{idx}].id={got:?} 不符合占位符格式 `$new_claim_<N>$`")]
    ClaimIdBadFormat { idx: usize, got: String },
    #[error(
        "new_claims[{idx}].id={got:?} 序号 {n} 与位置 {idx} 不一致；占位符必须从 0 开始按位置递增"
    )]
    ClaimIdNotSequential { idx: usize, n: usize, got: String },
    #[error("new_claims[{idx}] 缺少 name（用于派生 ClaimId）")]
    ClaimNameMissing { idx: usize },
    #[error("new_claims[{idx}] 缺少 scope（用于派生 ClaimId）")]
    ClaimScopeMissing { idx: usize },
    #[error("new_disputes[{idx}] 缺少 id 字段或 id 非字符串")]
    DisputeIdMissing { idx: usize },
    #[error("new_disputes[{idx}].id={got:?} 不符合占位符格式 `$new_dispute_<N>$`")]
    DisputeIdBadFormat { idx: usize, got: String },
    #[error("new_disputes[{idx}].id={got:?} 序号 {n} 与位置 {idx} 不一致")]
    DisputeIdNotSequential { idx: usize, n: usize, got: String },
    #[error("new_disputes[{idx}] 缺少 name（用于派生 DisputeId）")]
    DisputeNameMissing { idx: usize },
    #[error("new_disputes[{idx}] 缺少 summary（用于派生 DisputeId）")]
    DisputeSummaryMissing { idx: usize },
    #[error("new_disputes[{idx}] 缺少 claims 字段或不是数组")]
    DisputeClaimsMissing { idx: usize },
    #[error("new_disputes[{idx}].claims[{j}] 不是字符串")]
    DisputeClaimNotString { idx: usize, j: usize },
    #[error(
        "new_disputes[{idx}].claims[{j}]={got:?} 既不是真实 ClaimId 也不是已声明的占位符（未声明的引用？）"
    )]
    DisputeClaimUnresolved { idx: usize, j: usize, got: String },
    #[error("替换完成后仍残留未声明的占位符 `${kind}{n}$`，位置：{path}")]
    UnresolvedPlaceholder {
        kind: String,
        n: String,
        path: String,
    },
}

/// 解析整棵 Value 里的占位符并替换为真实 id。
///
/// `now` 用作派生 id 的纳秒时间戳；同一次响应里的所有 claim/dispute 共用同一个 now，
/// 让 runner 后续构造领域对象时也能复用同一个 created_at，保证 id 派生与落盘字段一致。
pub fn resolve_placeholders(
    mut value: Value,
    now: DateTime<Utc>,
) -> Result<Value, PlaceholderError> {
    let claim_subs = extract_claim_substitutions(&value, now)?;
    apply_substitutions(&mut value, &claim_subs);

    let dispute_subs = extract_dispute_substitutions(&value, now)?;
    apply_substitutions(&mut value, &dispute_subs);

    verify_no_unresolved(&value)?;
    Ok(value)
}

/// 抽取 `new_claims[*]` 的占位符 → 真实 ClaimId 映射。
fn extract_claim_substitutions(
    value: &Value,
    now: DateTime<Utc>,
) -> Result<BTreeMap<String, String>, PlaceholderError> {
    let mut subs = BTreeMap::new();
    let Some(claims) = value.get("new_claims").and_then(Value::as_array) else {
        return Ok(subs);
    };
    for (idx, claim) in claims.iter().enumerate() {
        let id = claim
            .get("id")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::ClaimIdMissing { idx })?;
        let n = parse_placeholder("$new_claim_", id).ok_or_else(|| {
            PlaceholderError::ClaimIdBadFormat {
                idx,
                got: id.to_string(),
            }
        })?;
        if n != idx {
            return Err(PlaceholderError::ClaimIdNotSequential {
                idx,
                n,
                got: id.to_string(),
            });
        }
        let name = claim
            .get("name")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::ClaimNameMissing { idx })?;
        let scope = claim
            .get("scope")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::ClaimScopeMissing { idx })?;
        let real_id = ClaimId::from_claim_parts(now, name, scope);
        subs.insert(id.to_string(), real_id.into_string());
    }
    Ok(subs)
}

/// 抽取 `new_disputes[*]` 的占位符 → 真实 DisputeId 映射。
/// 调用前 phase 1 已经把 claims 数组里的 `$new_claim_*$` 替换成真实 ClaimId，
/// 因此这里能直接 parse 出 ClaimId 喂给派生函数。
fn extract_dispute_substitutions(
    value: &Value,
    now: DateTime<Utc>,
) -> Result<BTreeMap<String, String>, PlaceholderError> {
    let mut subs = BTreeMap::new();
    let Some(disputes) = value.get("new_disputes").and_then(Value::as_array) else {
        return Ok(subs);
    };
    for (idx, dispute) in disputes.iter().enumerate() {
        let id = dispute
            .get("id")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::DisputeIdMissing { idx })?;
        let n = parse_placeholder("$new_dispute_", id).ok_or_else(|| {
            PlaceholderError::DisputeIdBadFormat {
                idx,
                got: id.to_string(),
            }
        })?;
        if n != idx {
            return Err(PlaceholderError::DisputeIdNotSequential {
                idx,
                n,
                got: id.to_string(),
            });
        }
        let name = dispute
            .get("name")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::DisputeNameMissing { idx })?;
        let claims_arr = dispute
            .get("claims")
            .and_then(Value::as_array)
            .ok_or(PlaceholderError::DisputeClaimsMissing { idx })?;
        let mut claims: Vec<ClaimId> = Vec::with_capacity(claims_arr.len());
        for (j, v) in claims_arr.iter().enumerate() {
            let s = v
                .as_str()
                .ok_or(PlaceholderError::DisputeClaimNotString { idx, j })?;
            let cid =
                s.parse::<ClaimId>()
                    .map_err(|_| PlaceholderError::DisputeClaimUnresolved {
                        idx,
                        j,
                        got: s.to_string(),
                    })?;
            claims.push(cid);
        }
        let summary = dispute
            .get("summary")
            .and_then(Value::as_str)
            .ok_or(PlaceholderError::DisputeSummaryMissing { idx })?;
        let real_id = DisputeId::from_dispute_parts(now, name, &claims, summary);
        subs.insert(id.to_string(), real_id.into_string());
    }
    Ok(subs)
}

/// 把 (占位符 → 真实 id) 映射广播替换到整棵 Value 树的所有字符串叶子。
fn apply_substitutions(value: &mut Value, subs: &BTreeMap<String, String>) {
    if subs.is_empty() {
        return;
    }
    walk_substitute(value, subs);
}

fn walk_substitute(value: &mut Value, subs: &BTreeMap<String, String>) {
    match value {
        Value::String(s) => {
            for (placeholder, real) in subs {
                if s.contains(placeholder.as_str()) {
                    *s = s.replace(placeholder.as_str(), real);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                walk_substitute(v, subs);
            }
        }
        Value::Object(obj) => {
            for (_, v) in obj.iter_mut() {
                walk_substitute(v, subs);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// 全树扫描残留占位符，发现即报错。
fn verify_no_unresolved(value: &Value) -> Result<(), PlaceholderError> {
    let mut path = String::from("$");
    walk_unresolved(value, &mut path)
}

fn walk_unresolved(value: &Value, path: &mut String) -> Result<(), PlaceholderError> {
    use std::fmt::Write;
    match value {
        Value::String(s) => {
            if let Some((kind, n)) = find_placeholder(s) {
                return Err(PlaceholderError::UnresolvedPlaceholder {
                    kind: kind.to_string(),
                    n,
                    path: path.clone(),
                });
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let prev = path.len();
                // path 里写下 [i]，递归后再裁回去；&mut String 不会触发分配（除非超容量）
                let _ = write!(path, "[{i}]");
                walk_unresolved(v, path)?;
                path.truncate(prev);
            }
        }
        Value::Object(obj) => {
            for (k, v) in obj {
                let prev = path.len();
                let _ = write!(path, ".{k}");
                walk_unresolved(v, path)?;
                path.truncate(prev);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

/// 在字符串里寻找首个 `$new_claim_<digits>$` 或 `$new_dispute_<digits>$`。
/// 找到返回 `(kind, "N")`；没找到返回 None。
fn find_placeholder(s: &str) -> Option<(&'static str, String)> {
    for kind in ["new_claim_", "new_dispute_"] {
        let pat = format!("${kind}");
        let mut search_from = 0usize;
        while let Some(rel) = s[search_from..].find(&pat) {
            let start = search_from + rel;
            let after = &s[start + pat.len()..];
            // 收集紧随其后的纯 ASCII 数字段
            let mut digit_end = 0usize;
            for (i, c) in after.char_indices() {
                if c.is_ascii_digit() {
                    digit_end = i + c.len_utf8();
                } else {
                    break;
                }
            }
            if digit_end > 0 && after[digit_end..].starts_with('$') {
                return Some((kind, after[..digit_end].to_string()));
            }
            // 这次没匹配上完整占位符（缺数字或缺尾 $），从下一个字符继续找
            search_from = start + 1;
        }
    }
    None
}

/// 解析单个完整占位符 `<prefix><digits>$` 取出数字部分。
/// 比如 `parse_placeholder("$new_claim_", "$new_claim_3$") == Some(3)`。
fn parse_placeholder(prefix: &str, full: &str) -> Option<usize> {
    let inner = full.strip_prefix(prefix)?;
    let inner = inner.strip_suffix('$')?;
    if inner.is_empty() || !inner.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    inner.parse::<usize>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 22, 10, 0, 0).unwrap()
    }

    fn input_single_claim() -> Value {
        serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_0$",
                "name": "payment_batch_timeout",
                "scope": "order-system / payment",
                "statement": "stmt",
                "confidence": "high",
                "evidence_summary": "evid",
                "source_claim_ids": []
            }],
            "used_claim_ids": [],
            "new_disputes": []
        })
    }

    #[test]
    fn single_claim_id_substituted_in_place() {
        let out = resolve_placeholders(input_single_claim(), now()).unwrap();
        let id = out["new_claims"][0]["id"].as_str().unwrap();
        assert!(id.starts_with("claim_"));
        // 长度 = 前缀 + 8 位 hex；具体值不固定（派生用 4 位随机 salt）
        assert_eq!(id.len(), "claim_".len() + 8);
        assert!(id.parse::<ClaimId>().is_ok());
    }

    #[test]
    fn claim_chain_via_source_claim_ids_resolves() {
        // claim 1 在 source_claim_ids 里引用 claim 0
        let v = serde_json::json!({
            "new_claims": [
                {
                    "id": "$new_claim_0$",
                    "name": "n0",
                    "scope": "s0",
                    "statement": "x",
                    "confidence": "medium",
                    "evidence_summary": "e",
                    "source_claim_ids": []
                },
                {
                    "id": "$new_claim_1$",
                    "name": "n1",
                    "scope": "s1",
                    "statement": "x",
                    "confidence": "medium",
                    "evidence_summary": "e",
                    "source_claim_ids": ["$new_claim_0$"]
                }
            ],
            "used_claim_ids": [],
            "new_disputes": []
        });
        let out = resolve_placeholders(v, now()).unwrap();
        let id0 = out["new_claims"][0]["id"].as_str().unwrap().to_string();
        let id1 = out["new_claims"][1]["id"].as_str().unwrap().to_string();
        let src = out["new_claims"][1]["source_claim_ids"][0]
            .as_str()
            .unwrap();
        assert_ne!(id0, id1);
        assert_eq!(src, id0);
    }

    #[test]
    fn dispute_with_two_new_claims_resolves() {
        let v = serde_json::json!({
            "new_claims": [
                {"id":"$new_claim_0$","name":"a","scope":"sa","statement":"","confidence":"high","evidence_summary":"","source_claim_ids":[]},
                {"id":"$new_claim_1$","name":"b","scope":"sb","statement":"","confidence":"high","evidence_summary":"","source_claim_ids":[]}
            ],
            "used_claim_ids": [],
            "new_disputes": [{
                "id": "$new_dispute_0$",
                "name": "d0",
                "claims": ["$new_claim_0$","$new_claim_1$"],
                "summary": "对比 $new_claim_0$ 与 $new_claim_1$"
            }]
        });
        let out = resolve_placeholders(v, now()).unwrap();
        let cid0 = out["new_claims"][0]["id"].as_str().unwrap();
        let cid1 = out["new_claims"][1]["id"].as_str().unwrap();
        let did0 = out["new_disputes"][0]["id"].as_str().unwrap();
        assert!(did0.starts_with("dispute_"));
        let claims = out["new_disputes"][0]["claims"].as_array().unwrap();
        assert_eq!(claims[0].as_str().unwrap(), cid0);
        assert_eq!(claims[1].as_str().unwrap(), cid1);
        // 自然语言里的占位符也被替换
        let summary = out["new_disputes"][0]["summary"].as_str().unwrap();
        assert!(summary.contains(cid0));
        assert!(summary.contains(cid1));
        assert!(!summary.contains("$new_claim_"));
    }

    #[test]
    fn placeholder_in_natural_language_evidence_summary_substituted() {
        let v = serde_json::json!({
            "new_claims": [
                {"id":"$new_claim_0$","name":"a","scope":"sa","statement":"","confidence":"high","evidence_summary":"","source_claim_ids":[]},
                {
                    "id": "$new_claim_1$",
                    "name": "b",
                    "scope": "sb",
                    "statement": "依据 $new_claim_0$ 推得",
                    "confidence": "medium",
                    "evidence_summary": "见 $new_claim_0$ 的论据",
                    "source_claim_ids": ["$new_claim_0$"]
                }
            ],
            "used_claim_ids": [],
            "new_disputes": []
        });
        let out = resolve_placeholders(v, now()).unwrap();
        let cid0 = out["new_claims"][0]["id"].as_str().unwrap();
        let stmt = out["new_claims"][1]["statement"].as_str().unwrap();
        let evid = out["new_claims"][1]["evidence_summary"].as_str().unwrap();
        assert!(stmt.contains(cid0));
        assert!(evid.contains(cid0));
    }

    #[test]
    fn unresolved_placeholder_in_source_claim_ids_errors() {
        let v = serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_0$",
                "name": "n",
                "scope": "s",
                "statement": "",
                "confidence": "high",
                "evidence_summary": "",
                "source_claim_ids": ["$new_claim_5$"]
            }],
            "used_claim_ids": [],
            "new_disputes": []
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(
            err,
            PlaceholderError::UnresolvedPlaceholder { .. }
        ));
        let msg = err.to_string();
        assert!(
            msg.contains("new_claim_") && msg.contains('5'),
            "实际: {msg}"
        );
    }

    #[test]
    fn unresolved_dispute_placeholder_in_natural_language_errors() {
        let v = serde_json::json!({
            "new_claims": [],
            "used_claim_ids": [],
            "new_disputes": [{
                "id": "$new_dispute_0$",
                "name": "d",
                "claims": [],
                "summary": "see $new_dispute_3$"
            }]
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(
            err,
            PlaceholderError::UnresolvedPlaceholder { .. }
        ));
    }

    #[test]
    fn nonsequential_claim_id_errors() {
        let v = serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_1$",
                "name": "n",
                "scope": "s",
                "statement": "",
                "confidence": "high",
                "evidence_summary": "",
                "source_claim_ids": []
            }]
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(
            err,
            PlaceholderError::ClaimIdNotSequential { idx: 0, n: 1, .. }
        ));
    }

    #[test]
    fn bad_format_placeholder_errors() {
        let v = serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_$",
                "name": "n",
                "scope": "s",
                "statement": "",
                "confidence": "high",
                "evidence_summary": "",
                "source_claim_ids": []
            }]
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(err, PlaceholderError::ClaimIdBadFormat { .. }));
    }

    #[test]
    fn duplicate_placeholder_index_caught_by_sequential_check() {
        // 两条 claim 都用 $new_claim_0$：第二条 idx=1 期望 N=1，但实际 N=0 → 报序号不一致
        let v = serde_json::json!({
            "new_claims": [
                {"id":"$new_claim_0$","name":"a","scope":"sa","statement":"","confidence":"high","evidence_summary":"","source_claim_ids":[]},
                {"id":"$new_claim_0$","name":"b","scope":"sb","statement":"","confidence":"high","evidence_summary":"","source_claim_ids":[]}
            ]
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(
            err,
            PlaceholderError::ClaimIdNotSequential { idx: 1, n: 0, .. }
        ));
    }

    #[test]
    fn existing_real_claim_id_in_source_passes_through() {
        let real = ClaimId::random().into_string();
        let v = serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_0$",
                "name": "n",
                "scope": "s",
                "statement": "",
                "confidence": "high",
                "evidence_summary": "",
                "source_claim_ids": [real.clone()]
            }]
        });
        let out = resolve_placeholders(v, now()).unwrap();
        assert_eq!(
            out["new_claims"][0]["source_claim_ids"][0]
                .as_str()
                .unwrap(),
            real
        );
    }

    #[test]
    fn empty_value_no_placeholders_is_no_op() {
        let v = serde_json::json!({"new_claims": [], "used_claim_ids": [], "new_disputes": []});
        let out = resolve_placeholders(v.clone(), now()).unwrap();
        assert_eq!(out, v);
    }

    #[test]
    fn value_without_keys_no_op() {
        let v = serde_json::json!({"foo": "bar"});
        let out = resolve_placeholders(v.clone(), now()).unwrap();
        assert_eq!(out, v);
    }

    #[test]
    fn multi_digit_index_supported() {
        // 0..12 共 12 条
        let mut claims = Vec::new();
        for i in 0..12 {
            claims.push(serde_json::json!({
                "id": format!("$new_claim_{i}$"),
                "name": format!("n{i}"),
                "scope": format!("s{i}"),
                "statement": "",
                "confidence": "low",
                "evidence_summary": "",
                "source_claim_ids": []
            }));
        }
        let v = serde_json::json!({"new_claims": claims});
        let out = resolve_placeholders(v, now()).unwrap();
        // claim 11 的 id 也已派生
        let id11 = out["new_claims"][11]["id"].as_str().unwrap();
        assert!(id11.starts_with("claim_"));
        // 不允许残留两位数占位符
        assert!(!serde_json::to_string(&out).unwrap().contains("$new_claim_"));
    }

    #[test]
    fn dollar_in_text_does_not_trigger_unresolved_check() {
        // 普通价格文本 "$5" 不应被误判为占位符
        let v = serde_json::json!({
            "new_claims": [{
                "id": "$new_claim_0$",
                "name": "n",
                "scope": "s",
                "statement": "价格 $5 elsewhere $100",
                "confidence": "high",
                "evidence_summary": "",
                "source_claim_ids": []
            }]
        });
        let out = resolve_placeholders(v, now()).unwrap();
        assert_eq!(
            out["new_claims"][0]["statement"].as_str().unwrap(),
            "价格 $5 elsewhere $100"
        );
    }

    #[test]
    fn dispute_claim_unresolved_when_undeclared_placeholder() {
        // dispute.claims[0] 引用了未声明的 claim 占位符——phase 1 不会替换它，
        // phase 2 解析 ClaimId 失败，应当报 DisputeClaimUnresolved
        let v = serde_json::json!({
            "new_claims": [],
            "new_disputes": [{
                "id": "$new_dispute_0$",
                "name": "d",
                "claims": ["$new_claim_9$","$new_claim_8$"],
                "summary": "x"
            }]
        });
        let err = resolve_placeholders(v, now()).unwrap_err();
        assert!(matches!(
            err,
            PlaceholderError::DisputeClaimUnresolved { .. }
        ));
    }

    #[test]
    fn parse_placeholder_basic() {
        assert_eq!(parse_placeholder("$new_claim_", "$new_claim_0$"), Some(0));
        assert_eq!(parse_placeholder("$new_claim_", "$new_claim_42$"), Some(42));
        assert_eq!(parse_placeholder("$new_claim_", "$new_claim_$"), None);
        assert_eq!(parse_placeholder("$new_claim_", "$new_claim_3"), None);
        assert_eq!(parse_placeholder("$new_claim_", "$new_dispute_3$"), None);
    }

    #[test]
    fn find_placeholder_locates_in_substring() {
        assert_eq!(
            find_placeholder("blah $new_claim_7$ blah"),
            Some(("new_claim_", "7".to_string()))
        );
        assert_eq!(
            find_placeholder("see $new_dispute_15$ for details"),
            Some(("new_dispute_", "15".to_string()))
        );
        assert_eq!(find_placeholder("nothing here"), None);
        assert_eq!(find_placeholder("$new_claim_"), None);
        assert_eq!(find_placeholder("$new_claim_$"), None);
    }

    #[test]
    fn find_placeholder_skips_partial_match_continues_search() {
        // 第一次找 `$new_claim_` 在 "$new_claim_x$" 处不完整；继续找到后面真正的
        let s = "noise $new_claim_x$ then $new_claim_3$ tail";
        assert_eq!(find_placeholder(s), Some(("new_claim_", "3".to_string())));
    }
}
