//! ID 类型与生成工具。
//!
//! 设计要点：
//! - PolicyId 由 maintainer 端单写者生成；落盘时用 `O_CREAT|O_EXCL` 占位查重，撞了重抽
//! - ClaimId / TraceId / DisputeId 在各 agent 侧本地派生，没有中央写者，所以走"纳秒时间戳 +
//!   语义字段 + 4 位随机 salt → 取 hash 低 32 位"路线，让跨 agent 同批碰撞概率降到几乎为零；
//!   三种派生 id 的骨架收敛在私有 helper [`derive_id_string`]，差异仅在自己的语义字段顺序
//! - inbox InboxId 仅 8 位 hex 随机，无去重需求
//! - MaintainerActionId（intent_xxx）标识 maintainer 一次对外动作（publish / deprecate /
//!   claim_update_suggestion），同一动作产生的多条 outbox entry 共享此 id；Maintainer 在写
//!   outbox 前会查重重抽，本模块只生成 8 位 hex 随机候选
//! - AgentId 由人类指定，校验 `^[a-z0-9_-]+$`，不参与随机生成
//! - "撞了重抽"是调用方职责（PolicyId 的查重在 maintainer / storage 层），id 模块只提供候选生成

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum IdError {
    #[error("ID 前缀错误: 期望 {expected}，实际 {actual}")]
    WrongPrefix {
        expected: &'static str,
        actual: String,
    },
    #[error("ID 后缀格式错误: {0}（必须是 8 位小写 hex）")]
    BadSuffix(String),
    #[error("AgentId 非法: {0}（必须匹配 ^[a-z0-9_-]+$ 且非空）")]
    BadAgentId(String),
    #[error("SourceId 前缀非法: {0}（必须以 claim_ 或 policy_ 开头）")]
    SourceIdBadPrefix(String),
}

/// 生成 8 位小写 hex 字符串（32 bit 随机）
fn random_hex8() -> String {
    let mut buf = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

/// 生成 4 位小写 hex salt（16 bit 随机）
fn random_hex4() -> String {
    let mut buf = [0u8; 2];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

macro_rules! prefixed_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            /// 生成一个新的随机候选 ID（不查重）
            pub fn random() -> Self {
                Self(format!("{}{}", $prefix, random_hex8()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl Ord for $name {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl PartialOrd for $name {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let suffix = s
                    .strip_prefix($prefix)
                    .ok_or_else(|| IdError::WrongPrefix {
                        expected: $prefix,
                        actual: s.to_string(),
                    })?;
                if suffix.len() != 8
                    || !suffix
                        .chars()
                        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
                {
                    return Err(IdError::BadSuffix(suffix.to_string()));
                }
                Ok(Self(s.to_string()))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
                let s = String::deserialize(de)?;
                Self::from_str(&s).map_err(serde::de::Error::custom)
            }
        }
    };
}

prefixed_id!(ClaimId, "claim_");
prefixed_id!(PolicyId, "policy_");
prefixed_id!(TraceId, "trace_");
prefixed_id!(DisputeId, "dispute_");
prefixed_id!(InboxId, "inbox_");
prefixed_id!(MaintainerActionId, "intent_");
prefixed_id!(SessionId, "session_");

impl ClaimId {
    /// 用纳秒级时间戳 + claim name + scope + 4 位随机 salt 派生 claim id 候选。
    /// 仍保持 `claim_` + 8 位 hex 格式；调用方负责同批和本地文件查重。
    pub fn from_claim_parts(created_at: DateTime<Utc>, name: &str, scope: &str) -> Self {
        Self::from_claim_parts_with_salt(created_at, name, scope, &random_hex4())
    }

    fn from_claim_parts_with_salt(
        created_at: DateTime<Utc>,
        name: &str,
        scope: &str,
        salt_hex4: &str,
    ) -> Self {
        Self(derive_id_string(Self::PREFIX, created_at, salt_hex4, |h| {
            name.hash(h);
            scope.hash(h);
        }))
    }
}

impl TraceId {
    /// 用纳秒级时间戳 + trace name + input sources + output_claim_ids + 4 位随机 salt 派生 trace id 候选。
    /// 与 ClaimId 同源逻辑：每次调用都会引入随机 salt，不以相同语义作为幂等依据。
    pub fn from_trace_parts(
        created_at: DateTime<Utc>,
        name: &str,
        input_sources: &[SourceId],
        output_claim_ids: &[ClaimId],
    ) -> Self {
        Self::from_trace_parts_with_salt(
            created_at,
            name,
            input_sources,
            output_claim_ids,
            &random_hex4(),
        )
    }

    fn from_trace_parts_with_salt(
        created_at: DateTime<Utc>,
        name: &str,
        input_sources: &[SourceId],
        output_claim_ids: &[ClaimId],
        salt_hex4: &str,
    ) -> Self {
        Self(derive_id_string(Self::PREFIX, created_at, salt_hex4, |h| {
            name.hash(h);
            for id in input_sources {
                id.as_str().hash(h);
            }
            // 用分隔字节防止 input/output 边界拼接歧义（[a,b],[] 与 [a],[b] 必须 hash 不同）
            0u8.hash(h);
            for id in output_claim_ids {
                id.as_str().hash(h);
            }
        }))
    }
}

impl DisputeId {
    /// 用纳秒级时间戳 + dispute name + claim ids + summary + 4 位随机 salt 派生 dispute id 候选。
    /// 与 ClaimId / TraceId 同源逻辑：dispute 由 agent 侧生成，没有中央写者可查重，
    /// 故走派生路线让跨 agent 同批碰撞概率降到几乎为零。
    pub fn from_dispute_parts(
        created_at: DateTime<Utc>,
        name: &str,
        claims: &[ClaimId],
        summary: &str,
    ) -> Self {
        Self::from_dispute_parts_with_salt(created_at, name, claims, summary, &random_hex4())
    }

    fn from_dispute_parts_with_salt(
        created_at: DateTime<Utc>,
        name: &str,
        claims: &[ClaimId],
        summary: &str,
        salt_hex4: &str,
    ) -> Self {
        Self(derive_id_string(Self::PREFIX, created_at, salt_hex4, |h| {
            name.hash(h);
            for id in claims {
                id.as_str().hash(h);
            }
            // 这里不显式塞分隔字节：`<&str as Hash>` 自身在结尾追加 0xff 终结符，且 UTF-8 不允许
            // 0xff 出现在内容里，所以 ClaimId 字符串与 summary 之间天然不会拼接歧义。
            // （TraceId 的 input/output 都是 ClaimId 列表，empty 列表无终结符可写，必须显式分隔；
            //   DisputeId 的 summary 是 &str，无此问题。）
            summary.hash(h);
        }))
    }
}

/// 三种 agent 侧派生 id 共用的骨架：纳秒时间戳 → 调用方塞自己的语义字段 → 4 位 salt →
/// 取 `DefaultHasher` 输出低 32 位 → 拼成 `<prefix><8 位 hex>`。
/// 写成闭包接收 payload 是为了把"hash 顺序"这块可变部分留给 caller，骨架本身只维护一份。
fn derive_id_string<F>(
    prefix: &'static str,
    created_at: DateTime<Utc>,
    salt_hex4: &str,
    hash_payload: F,
) -> String
where
    F: FnOnce(&mut DefaultHasher),
{
    let nanos = created_at
        .timestamp_nanos_opt()
        .unwrap_or_else(|| created_at.timestamp() * 1_000_000_000);
    let mut hasher = DefaultHasher::new();
    nanos.hash(&mut hasher);
    hash_payload(&mut hasher);
    salt_hex4.hash(&mut hasher);
    // 截取 hash 低 32 位作为 8 位 hex 后缀；高位丢失是预期行为，仅用于显示，不参与等值判断
    #[allow(
        clippy::cast_possible_truncation,
        reason = "故意截取 hash 低 32 位生成短 ID"
    )]
    let suffix = hasher.finish() as u32;
    format!("{prefix}{suffix:08x}")
}

/// 一条 claim 的来源 id：可能是另一条 claim（claim_xxx）或一条 policy（policy_xxx）。
///
/// 序列化形态与底层 ClaimId / PolicyId 完全一致——单个字符串、无 enum 标签——
/// 所以磁盘上的 YAML 文件保持 `source_claim_ids: [claim_xxx, policy_yyy]` 形态。
/// 反序列化时根据前缀分发到对应变体；前缀既不是 claim_ 也不是 policy_ 时报错，
/// 把过去 `Vec<String>` 的"任何字符串都能塞进去"的弱契约升级为类型层强保障。
///
/// 不使用 `#[serde(untagged)]`：底层 ClaimId / PolicyId 用 `#[serde(transparent)]`，
/// derive 出来的 Deserialize 不验证前缀，untagged 会让第一个变体直接吞掉所有字符串。
/// 因此这里手写 Serialize / Deserialize 走 Display / FromStr。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SourceId {
    Claim(ClaimId),
    Policy(PolicyId),
}

impl SourceId {
    pub fn as_str(&self) -> &str {
        match self {
            SourceId::Claim(c) => c.as_str(),
            SourceId::Policy(p) => p.as_str(),
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(ClaimId::PREFIX) {
            ClaimId::from_str(s).map(SourceId::Claim)
        } else if s.starts_with(PolicyId::PREFIX) {
            PolicyId::from_str(s).map(SourceId::Policy)
        } else {
            Err(IdError::SourceIdBadPrefix(s.to_string()))
        }
    }
}

impl Serialize for SourceId {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl From<ClaimId> for SourceId {
    fn from(c: ClaimId) -> Self {
        SourceId::Claim(c)
    }
}

impl From<PolicyId> for SourceId {
    fn from(p: PolicyId) -> Self {
        SourceId::Policy(p)
    }
}

/// agent 标识，人类指定，校验 `^[a-z0-9_-]+$`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct AgentId(String);

impl AgentId {
    pub fn new(s: impl Into<String>) -> Result<Self, IdError> {
        let s = s.into();
        if s.is_empty()
            || !s
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
        {
            return Err(IdError::BadAgentId(s));
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for AgentId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn claim_id_format() {
        let id = ClaimId::random();
        assert!(id.as_str().starts_with("claim_"));
        assert_eq!(id.as_str().len(), "claim_".len() + 8);
        assert!(ClaimId::from_str(id.as_str()).is_ok());
    }

    #[test]
    fn claim_id_from_parts_with_same_salt_is_stable_and_8_hex() {
        let t = "2026-04-22T10:00:00.123456789Z".parse().unwrap();
        let id1 = ClaimId::from_claim_parts_with_salt(
            t,
            "payment_batch_timeout",
            "order-system / payment",
            "0a1b",
        );
        let id2 = ClaimId::from_claim_parts_with_salt(
            t,
            "payment_batch_timeout",
            "order-system / payment",
            "0a1b",
        );
        assert_eq!(id1, id2);
        assert_eq!(id1.as_str().len(), "claim_".len() + 8);
        assert!(ClaimId::from_str(id1.as_str()).is_ok());
    }

    #[test]
    fn claim_id_from_parts_changes_with_nanos_or_scope() {
        // 仅相差 1 纳秒的两个时间戳应得到不同 id
        let t1: DateTime<Utc> = "2026-04-22T10:00:00.000000001Z".parse().unwrap();
        let t2: DateTime<Utc> = "2026-04-22T10:00:00.000000002Z".parse().unwrap();
        let id1 = ClaimId::from_claim_parts_with_salt(t1, "n", "scope-a", "0a1b");
        let id2 = ClaimId::from_claim_parts_with_salt(t2, "n", "scope-a", "0a1b");
        let id3 = ClaimId::from_claim_parts_with_salt(t1, "n", "scope-b", "0a1b");
        let id4 = ClaimId::from_claim_parts_with_salt(t1, "n", "scope-a", "0a1c");
        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_ne!(id1, id4);
    }

    #[test]
    fn trace_id_from_parts_with_same_salt_is_stable_and_8_hex() {
        let t = "2026-04-22T10:00:00.123456789Z".parse().unwrap();
        let inputs = vec![
            SourceId::Claim(ClaimId::random()),
            SourceId::Policy(PolicyId::random()),
        ];
        let outputs = vec![ClaimId::random()];
        let id1 = TraceId::from_trace_parts_with_salt(t, "batch_retry", &inputs, &outputs, "0a1b");
        let id2 = TraceId::from_trace_parts_with_salt(t, "batch_retry", &inputs, &outputs, "0a1b");
        assert_eq!(id1, id2);
        assert_eq!(id1.as_str().len(), "trace_".len() + 8);
        assert!(TraceId::from_str(id1.as_str()).is_ok());
    }

    #[test]
    fn trace_id_distinguishes_input_and_output_split() {
        // [a,b],[] 与 [a],[b] 在不带分隔时会 hash 出同样结果；本测试守住分隔字节的存在
        let t: DateTime<Utc> = "2026-04-22T10:00:00.000000001Z".parse().unwrap();
        let a = SourceId::Claim(ClaimId::random());
        let b = SourceId::Policy(PolicyId::random());
        let output = ClaimId::random();
        let id_left =
            TraceId::from_trace_parts_with_salt(t, "n", &[a.clone(), b.clone()], &[], "0a1b");
        let id_split = TraceId::from_trace_parts_with_salt(t, "n", &[a], &[output], "0a1b");
        assert_ne!(id_left, id_split);
    }

    #[test]
    fn dispute_id_from_parts_with_same_salt_is_stable_and_8_hex() {
        let t = "2026-04-22T10:00:00.123456789Z".parse().unwrap();
        let claims = vec![ClaimId::random(), ClaimId::random()];
        let id1 = DisputeId::from_dispute_parts_with_salt(t, "n", &claims, "summary", "0a1b");
        let id2 = DisputeId::from_dispute_parts_with_salt(t, "n", &claims, "summary", "0a1b");
        assert_eq!(id1, id2);
        assert_eq!(id1.as_str().len(), "dispute_".len() + 8);
        assert!(DisputeId::from_str(id1.as_str()).is_ok());
    }

    #[test]
    fn dispute_id_from_parts_changes_with_each_field() {
        let t1: DateTime<Utc> = "2026-04-22T10:00:00.000000001Z".parse().unwrap();
        let t2: DateTime<Utc> = "2026-04-22T10:00:00.000000002Z".parse().unwrap();
        let claims = vec![ClaimId::random(), ClaimId::random()];
        let other_claims = vec![ClaimId::random()];
        let base = DisputeId::from_dispute_parts_with_salt(t1, "n", &claims, "s", "0a1b");
        // 任一字段变化都应得到不同 id
        assert_ne!(
            base,
            DisputeId::from_dispute_parts_with_salt(t2, "n", &claims, "s", "0a1b")
        );
        assert_ne!(
            base,
            DisputeId::from_dispute_parts_with_salt(t1, "n2", &claims, "s", "0a1b")
        );
        assert_ne!(
            base,
            DisputeId::from_dispute_parts_with_salt(t1, "n", &other_claims, "s", "0a1b")
        );
        assert_ne!(
            base,
            DisputeId::from_dispute_parts_with_salt(t1, "n", &claims, "s2", "0a1b")
        );
        assert_ne!(
            base,
            DisputeId::from_dispute_parts_with_salt(t1, "n", &claims, "s", "0a1c")
        );
    }

    #[test]
    fn dispute_id_changes_with_summary_only() {
        // sanity check：仅 summary 不同也应得到不同 id（其他字段被 dispute_id_from_parts_changes_with_each_field
        // 全量覆盖；此处单独留一条是为了防回归——历史上曾考虑过给 claims/summary 之间塞分隔字节，
        // 后来确认 `<&str as Hash>` + UTF-8 已经天然消除拼接歧义，分隔字节被删；这条断言保住的是
        // "派生方法对 summary 字段敏感"这条最低不变量。
        let t: DateTime<Utc> = "2026-04-22T10:00:00.000000001Z".parse().unwrap();
        let a = ClaimId::random();
        let id1 = DisputeId::from_dispute_parts_with_salt(
            t,
            "n",
            std::slice::from_ref(&a),
            "abc",
            "0a1b",
        );
        let id2 = DisputeId::from_dispute_parts_with_salt(t, "n", &[a], "xabc", "0a1b");
        assert_ne!(id1, id2);
    }

    #[test]
    fn id_round_trip_via_serde_yaml() {
        let id = DisputeId::random();
        let yaml = serde_yaml_ng::to_string(&id).unwrap();
        let back: DisputeId = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn serde_rejects_invalid_prefixed_id() {
        let err = serde_yaml_ng::from_str::<ClaimId>("not_a_claim").unwrap_err();
        assert!(err.to_string().contains("ID 前缀错误"));

        let err = serde_yaml_ng::from_str::<PolicyId>("policy_ABCDEF12").unwrap_err();
        assert!(err.to_string().contains("ID 后缀格式错误"));
    }

    #[test]
    fn agent_id_validation() {
        assert!(AgentId::new("agent-a").is_ok());
        assert!(AgentId::new("agent_a").is_ok());
        assert!(AgentId::new("").is_err());
        assert!(AgentId::new("Agent").is_err()); // 大写非法
        assert!(AgentId::new("agent.a").is_err()); // 点号非法，避免路径含义歧义
    }

    #[test]
    fn serde_rejects_invalid_agent_id() {
        let err = serde_yaml_ng::from_str::<AgentId>("Agent").unwrap_err();
        assert!(err.to_string().contains("AgentId 非法"));
    }

    #[test]
    fn random_ids_are_likely_unique() {
        // 1024 次抽样不应有重复（碰撞概率极低，仅做 sanity check）
        let mut set = HashSet::new();
        for _ in 0..1024 {
            assert!(set.insert(ClaimId::random()));
        }
    }

    #[test]
    fn wrong_prefix_rejected() {
        assert!(matches!(
            ClaimId::from_str("policy_12345678"),
            Err(IdError::WrongPrefix { .. })
        ));
    }

    #[test]
    fn bad_suffix_rejected() {
        assert!(matches!(
            ClaimId::from_str("claim_XYZ"),
            Err(IdError::BadSuffix(_))
        ));
        assert!(matches!(
            ClaimId::from_str("claim_ABCDEF12"),
            Err(IdError::BadSuffix(_))
        )); // 大写也不接受
    }

    #[test]
    fn source_id_parses_claim_and_policy_prefixes() {
        let c = ClaimId::random();
        let p = PolicyId::random();
        let s_c: SourceId = c.as_str().parse().unwrap();
        let s_p: SourceId = p.as_str().parse().unwrap();
        assert!(matches!(s_c, SourceId::Claim(ref id) if id == &c));
        assert!(matches!(s_p, SourceId::Policy(ref id) if id == &p));
    }

    #[test]
    fn source_id_rejects_unknown_prefix() {
        let err = "trace_aabbccdd".parse::<SourceId>().unwrap_err();
        assert!(matches!(err, IdError::SourceIdBadPrefix(_)));
    }

    #[test]
    fn source_id_rejects_bad_suffix_via_inner() {
        // 前缀对但后缀 4 位 → 走到 ClaimId::from_str 失败，包成 IdError::BadSuffix
        let err = "claim_xy".parse::<SourceId>().unwrap_err();
        assert!(matches!(err, IdError::BadSuffix(_)));
    }

    #[test]
    fn source_id_serializes_as_plain_string_yaml() {
        let c = ClaimId::random();
        let s = SourceId::Claim(c.clone());
        let yaml = serde_yaml_ng::to_string(&s).unwrap();
        // 期望是单行字符串，不是 enum-tag 形态
        assert!(yaml.trim() == c.as_str());
        let back: SourceId = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn source_id_round_trip_in_vec_yaml_preserves_disk_format() {
        let c = ClaimId::random();
        let p = PolicyId::random();
        let v: Vec<SourceId> = vec![c.clone().into(), p.clone().into()];
        let yaml = serde_yaml_ng::to_string(&v).unwrap();
        // 防回归：磁盘形态必须是裸字符串列表，不允许出现 enum 标签如 `Claim:` / `Policy:`
        assert!(yaml.contains(c.as_str()));
        assert!(yaml.contains(p.as_str()));
        assert!(!yaml.contains("Claim"));
        assert!(!yaml.contains("Policy"));
        let back: Vec<SourceId> = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn source_id_deserialize_from_legacy_string_list() {
        // 守护：旧版本写盘的 Vec<String> 形态应该能直接被新 Vec<SourceId> 读回
        let c = ClaimId::random();
        let p = PolicyId::random();
        let yaml = format!("- {}\n- {}\n", c.as_str(), p.as_str());
        let back: Vec<SourceId> = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(back, vec![SourceId::Claim(c), SourceId::Policy(p)]);
    }
}
