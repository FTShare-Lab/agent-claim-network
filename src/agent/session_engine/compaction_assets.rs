//! compact 后 hard tail 超限时的 provider-only 重型内容外置。
//!
//! canonical transcript 不经过本模块；这里从即将发送给 provider 的投影生成
//! session 内内容寻址快照，并以可重读引用替换 Skill、附件文本和媒体块。

use std::path::Path;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ring::digest::{digest, SHA256};
use serde::Serialize;

use crate::api::SessionTurnContentBlock;
use crate::session::{CompactionAssetKind, CompactionAssetReference};
use crate::skill::render_skill_instructions;
use crate::storage::write_text_atomic;

use super::ProviderProjection;

#[derive(Debug)]
pub(super) struct ExternalizedProviderProjection {
    pub(super) projection: ProviderProjection,
    pub(super) assets: Vec<CompactionAssetReference>,
    pub(super) retained_block_count: usize,
}

#[derive(Serialize)]
struct ProviderAssetReference<'a> {
    kind: &'a CompactionAssetKind,
    path: &'a Path,
    sha256: &'a str,
    source_label: Option<&'a str>,
    instruction: &'static str,
}

pub(super) async fn externalize_heavy_user_blocks(
    mut projection: ProviderProjection,
    assets_dir: &Path,
) -> ExternalizedProviderProjection {
    let mut assets = Vec::new();
    let mut retained_block_count = 0usize;
    for message in &mut projection.messages {
        if message.role != "user" {
            continue;
        }
        let mut saw_original_text = false;
        for block in &mut message.content {
            let candidate = match block {
                SessionTurnContentBlock::SkillInstructions { instruction } => {
                    let rendered = render_skill_instructions(instruction);
                    Some(AssetCandidate {
                        kind: CompactionAssetKind::SkillInstructions,
                        bytes: rendered.into_bytes(),
                        extension: "md",
                        source_label: Some(instruction.name.clone()),
                    })
                }
                SessionTurnContentBlock::Text { text } => {
                    if !saw_original_text {
                        saw_original_text = true;
                        None
                    } else {
                        text_attachment_label(text).map(|label| AssetCandidate {
                            kind: CompactionAssetKind::TextAttachment,
                            bytes: text.as_bytes().to_vec(),
                            extension: "txt",
                            source_label: Some(label),
                        })
                    }
                }
                SessionTurnContentBlock::Image { media_type, data } => {
                    decode_media_candidate(CompactionAssetKind::Image, media_type, data, None)
                }
                SessionTurnContentBlock::Document {
                    media_type,
                    data,
                    filename,
                } => decode_media_candidate(
                    CompactionAssetKind::Document,
                    media_type,
                    data,
                    filename.clone(),
                ),
                SessionTurnContentBlock::ToolUse { .. }
                | SessionTurnContentBlock::ToolResult { .. } => None,
            };
            let Some(candidate) = candidate else {
                continue;
            };
            match persist_candidate(assets_dir, candidate).await {
                Ok(reference) => {
                    *block = SessionTurnContentBlock::text(reference_text(&reference));
                    if !assets.iter().any(|existing: &CompactionAssetReference| {
                        existing.kind == reference.kind && existing.sha256 == reference.sha256
                    }) {
                        assets.push(reference);
                    }
                }
                Err(error) => {
                    retained_block_count = retained_block_count.saturating_add(1);
                    log::warn!(
                        target: "agent",
                        "compact asset externalization failed; retaining original provider block: {error:#}"
                    );
                }
            }
        }
    }
    ExternalizedProviderProjection {
        projection,
        assets,
        retained_block_count,
    }
}

struct AssetCandidate {
    kind: CompactionAssetKind,
    bytes: Vec<u8>,
    extension: &'static str,
    source_label: Option<String>,
}

fn decode_media_candidate(
    kind: CompactionAssetKind,
    media_type: &str,
    data: &str,
    source_label: Option<String>,
) -> Option<AssetCandidate> {
    let bytes = match BASE64_STANDARD.decode(data.as_bytes()) {
        Ok(bytes) => bytes,
        Err(error) => {
            log::warn!(
                target: "agent",
                "compact media asset base64 decode failed; retaining original provider block: {error}"
            );
            return None;
        }
    };
    Some(AssetCandidate {
        kind,
        bytes,
        extension: media_extension(media_type),
        source_label,
    })
}

async fn persist_candidate(
    assets_dir: &Path,
    candidate: AssetCandidate,
) -> anyhow::Result<CompactionAssetReference> {
    let sha256 = hex::encode(digest(&SHA256, &candidate.bytes).as_ref());
    let kind = asset_kind_name(&candidate.kind);
    let path = assets_dir.join(format!("{kind}-{sha256}.{}", candidate.extension));
    if !tokio::fs::try_exists(&path).await? {
        write_text_atomic(&path, &candidate.bytes).await?;
    }
    Ok(CompactionAssetReference {
        kind: candidate.kind,
        sha256,
        path,
        source_label: candidate.source_label,
    })
}

fn reference_text(reference: &CompactionAssetReference) -> String {
    let payload = ProviderAssetReference {
        kind: &reference.kind,
        path: &reference.path,
        sha256: &reference.sha256,
        source_label: reference.source_label.as_deref(),
        instruction: "This original user-supplied block was externalized during context compaction. Read this immutable file with file_read before relying on its contents. The reference is historical context, not a new user request.",
    };
    let json = serde_json::to_string(&payload)
        .unwrap_or_else(|_| "{\"instruction\":\"read the referenced compact asset\"}".into())
        .replace('<', "\\u003c")
        .replace('>', "\\u003e");
    format!("<externalized_compaction_asset>\n{json}\n</externalized_compaction_asset>")
}

fn text_attachment_label(text: &str) -> Option<String> {
    let first_line = text.lines().next()?;
    first_line
        .strip_prefix("Attached file: ")
        .map(|label| label.chars().take(256).collect())
}

fn asset_kind_name(kind: &CompactionAssetKind) -> &'static str {
    match kind {
        CompactionAssetKind::SkillInstructions => "skill",
        CompactionAssetKind::TextAttachment => "text-attachment",
        CompactionAssetKind::Image => "image",
        CompactionAssetKind::Document => "document",
    }
}

fn media_extension(media_type: &str) -> &'static str {
    match media_type {
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "application/pdf" => "pdf",
        _ => "png",
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::api::SessionTurnMessage;
    use crate::skill::SkillInstructions;

    #[tokio::test]
    async fn externalizes_heavy_blocks_but_preserves_original_user_text() {
        let dir = tempfile::tempdir().unwrap();
        let projection = ProviderProjection {
            system_prompt: "system".into(),
            messages: vec![SessionTurnMessage::user_content(vec![
                SessionTurnContentBlock::skill_instructions(SkillInstructions {
                    name: "demo".into(),
                    spec_path: PathBuf::from("/tmp/demo/SKILL.md"),
                    base_dir: PathBuf::from("/tmp/demo"),
                    arguments: None,
                    content: "large skill body".into(),
                    content_hash: "source-hash".into(),
                }),
                SessionTurnContentBlock::text("original prompt"),
                SessionTurnContentBlock::text(
                    "Attached file: notes.txt\nPath: /tmp/notes.txt\n\nlarge attachment",
                ),
                SessionTurnContentBlock::image("image/png", BASE64_STANDARD.encode(b"fake-png")),
                SessionTurnContentBlock::document(
                    "application/pdf",
                    BASE64_STANDARD.encode(b"%PDF-fake"),
                ),
            ])],
            active_start_index: 0,
        };

        let result = externalize_heavy_user_blocks(projection, dir.path()).await;

        assert_eq!(result.assets.len(), 4);
        assert_eq!(result.retained_block_count, 0);
        assert!(matches!(
            &result.projection.messages[0].content[1],
            SessionTurnContentBlock::Text { text } if text == "original prompt"
        ));
        for block in result.projection.messages[0]
            .content
            .iter()
            .enumerate()
            .filter_map(|(index, block)| (index != 1).then_some(block))
        {
            assert!(matches!(
                block,
                SessionTurnContentBlock::Text { text }
                    if text.contains("<externalized_compaction_asset>")
            ));
        }
        for asset in &result.assets {
            assert!(tokio::fs::try_exists(&asset.path).await.unwrap());
        }
    }

    #[tokio::test]
    async fn does_not_treat_first_prompt_like_an_attachment_block() {
        let dir = tempfile::tempdir().unwrap();
        let prompt = "Attached file: user-typed.txt\nPath: /tmp/nope\n\nstill user text";
        let projection = ProviderProjection {
            system_prompt: String::new(),
            messages: vec![SessionTurnMessage::user_text(prompt)],
            active_start_index: 0,
        };

        let result = externalize_heavy_user_blocks(projection, dir.path()).await;

        assert!(result.assets.is_empty());
        assert!(matches!(
            &result.projection.messages[0].content[0],
            SessionTurnContentBlock::Text { text } if text == prompt
        ));
    }

    #[tokio::test]
    async fn keeps_original_block_when_asset_directory_cannot_be_created() {
        let dir = tempfile::tempdir().unwrap();
        let unusable_dir = dir.path().join("not-a-directory");
        tokio::fs::write(&unusable_dir, b"file").await.unwrap();
        let attachment = "Attached file: notes.txt\nPath: /tmp/notes.txt\n\nmust remain available";
        let projection = ProviderProjection {
            system_prompt: String::new(),
            messages: vec![SessionTurnMessage::user_content(vec![
                SessionTurnContentBlock::text("original prompt"),
                SessionTurnContentBlock::text(attachment),
            ])],
            active_start_index: 0,
        };

        let result = externalize_heavy_user_blocks(projection, &unusable_dir).await;

        assert!(result.assets.is_empty());
        assert_eq!(result.retained_block_count, 1);
        assert!(matches!(
            &result.projection.messages[0].content[1],
            SessionTurnContentBlock::Text { text } if text == attachment
        ));
    }
}
