//! serde 反序列化小工具。
//!
//! 只放跨模块复用的格式兼容函数；本地持久化结构默认仍保持严格校验。

use serde::Deserialize;

/// 把字段缺失或显式 `null` 都当作目标类型的默认值。
pub(crate) fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}
