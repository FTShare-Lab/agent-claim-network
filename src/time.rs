//! 时间序列化辅助。
//!
//! 内存计算使用 `chrono::DateTime<Utc>`，文件落盘时输出干净的 RFC 3339 字符串
//! （形如 `2026-04-21T18:30:00Z`，秒级精度时不输出小数；带亚秒精度时按需保留）。
//! chrono 默认 serde 会写到纳秒精度（`.000000000Z`），可读性差，因此本模块统一替换。
//! 反序列化接受任何带时区的 RFC 3339（`Z` / `+08:00` 都行），归一化回 UTC。

use chrono::{DateTime, SecondsFormat, Utc};

fn format_utc(dt: DateTime<Utc>) -> String {
    // AutoSi：纯秒精度时不输出小数；带亚秒时按需保留毫秒/微秒/纳秒
    // use_z=true → 用 `Z` 后缀而不是 `+00:00`
    dt.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

/// 把任意 `DateTime<Utc>` 截断到整秒。
/// 原始时钟在 macOS/Linux 上一般是纳秒精度，落盘后会出现 `.123456789Z` 这种尾巴，可读性差；
/// 业务实体的时间戳一律走这里抹掉亚秒。`from_timestamp(s, 0)` 在合法 i64 秒下不会失败，
/// 极端情况下回退到入参本身，保证调用点不需要处理 `Result`。
pub fn truncate_to_second(t: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(t.timestamp(), 0).unwrap_or(t)
}

/// 等价于 `truncate_to_second(Utc::now())`：业务实体写入时间统一调用此函数。
/// 唯一例外：`ClaimId::from_claim_parts` / `TraceId::from_trace_parts` 需要纳秒级精度
/// 作 hash 输入，那里在内部直接用纳秒时间戳，不经此截断。
pub fn now_seconds() -> DateTime<Utc> {
    truncate_to_second(Utc::now())
}

/// serde with 模块：把 `DateTime<Utc>` 字段以 RFC 3339 (UTC, Z 后缀) 写入 / 读取。
pub mod serde_utc {
    use super::format_utc;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(dt: &DateTime<Utc>, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&format_utc(*dt))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(de)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// `serde_utc` 的 `Option` 版本，用于 `Option<DateTime<Utc>>` 字段。
pub mod serde_utc_opt {
    use super::format_utc;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(dt: &Option<DateTime<Utc>>, ser: S) -> Result<S::Ok, S::Error> {
        match dt {
            Some(dt) => ser.serialize_str(&format_utc(*dt)),
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error> {
        let s: Option<String> = Option::deserialize(de)?;
        s.map(|s| {
            DateTime::parse_from_rfc3339(&s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(serde::de::Error::custom)
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Foo {
        #[serde(with = "serde_utc")]
        at: DateTime<Utc>,
        #[serde(
            default,
            with = "serde_utc_opt",
            skip_serializing_if = "Option::is_none"
        )]
        maybe_at: Option<DateTime<Utc>>,
    }

    #[test]
    fn serializes_required_field_z_suffix_seconds() {
        let f = Foo {
            at: "2026-04-21T08:00:00Z".parse().unwrap(),
            maybe_at: None,
        };
        let yaml = serde_yaml_ng::to_string(&f).unwrap();
        assert!(yaml.contains("at: 2026-04-21T08:00:00Z"), "yaml=\n{yaml}");
        // None 字段被 skip
        assert!(!yaml.contains("maybe_at"));
    }

    #[test]
    fn serializes_optional_field_z_suffix_seconds() {
        let f = Foo {
            at: "2026-04-21T08:00:00Z".parse().unwrap(),
            maybe_at: Some("2026-04-21T09:30:00Z".parse().unwrap()),
        };
        let yaml = serde_yaml_ng::to_string(&f).unwrap();
        assert!(
            yaml.contains("maybe_at: 2026-04-21T09:30:00Z"),
            "yaml=\n{yaml}"
        );
    }

    #[test]
    fn round_trip_preserves_instant_with_subsecond() {
        let f = Foo {
            at: "2026-04-21T12:34:56Z".parse().unwrap(),
            maybe_at: Some("2026-04-21T12:34:56.123Z".parse().unwrap()),
        };
        let yaml = serde_yaml_ng::to_string(&f).unwrap();
        let back: Foo = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn deserialize_accepts_offset_form() {
        // 兼容第三方写入：+08:00 形式仍能解析并正确归一到 UTC
        let yaml = "at: 2026-04-21T08:00:00+08:00\n";
        let f: Foo = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            f.at,
            "2026-04-21T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn auto_si_keeps_subsecond_precision_only_when_present() {
        let secs = format_utc("2026-04-21T00:00:00Z".parse().unwrap());
        assert_eq!(secs, "2026-04-21T00:00:00Z");
        let ms = format_utc("2026-04-21T00:00:00.250Z".parse().unwrap());
        assert_eq!(ms, "2026-04-21T00:00:00.250Z");
    }

    #[test]
    fn truncate_to_second_drops_subsecond() {
        let t: DateTime<Utc> = "2026-04-21T12:34:56.789123456Z".parse().unwrap();
        let s = truncate_to_second(t);
        assert_eq!(s.timestamp(), t.timestamp());
        assert_eq!(s.timestamp_subsec_nanos(), 0);
        assert_eq!(format_utc(s), "2026-04-21T12:34:56Z");
    }

    #[test]
    fn truncate_to_second_idempotent_on_whole_second() {
        let t: DateTime<Utc> = "2026-04-21T12:34:56Z".parse().unwrap();
        assert_eq!(truncate_to_second(t), t);
    }

    #[test]
    fn now_seconds_has_no_subsecond() {
        // 真实系统时钟：取两次都应为整秒
        for _ in 0..4 {
            assert_eq!(now_seconds().timestamp_subsec_nanos(), 0);
        }
    }
}
