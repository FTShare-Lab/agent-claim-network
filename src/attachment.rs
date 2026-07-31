//! 附件公共层：读取、校验、规格化图片 / PDF / 文本附件。
//!
//! 三个入口复用同一套实现：TUI 内联 `@path`、`file_read` 工具的二进制分支、
//! macOS 剪贴板图片（直接走 `normalize_image_attachment`）。
//! 对外核心类型：`AttachmentLimits` / `AttachmentKind` / `NormalizedMedia` / `AttachmentError`。

use std::io::Cursor;
use std::path::Path;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use image::{ImageFormat, ImageReader};
use serde_json::{json, Value};

use crate::config::{
    AttachmentConfig, DEFAULT_ATTACHMENT_MAX_FILES_PER_TURN, DEFAULT_ATTACHMENT_MAX_FILE_BYTES,
};

/// 图片规格化后的最大边长（像素）。协议内部不变量，刻意不暴露为配置。
const IMAGE_MAX_EDGE_PX: u32 = 2048;
const IMAGE_DOWNSAMPLE_EDGE_PX: &[u32] = &[2048, 1536, 1024, 768, 512];

/// 附件尺寸 / 数量限制，来源于 `[agent.attachment]` 配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachmentLimits {
    pub enabled: bool,
    pub max_file_bytes: u64,
    pub max_files_per_turn: usize,
}

impl Default for AttachmentLimits {
    fn default() -> Self {
        Self {
            enabled: true,
            max_file_bytes: DEFAULT_ATTACHMENT_MAX_FILE_BYTES,
            max_files_per_turn: DEFAULT_ATTACHMENT_MAX_FILES_PER_TURN,
        }
    }
}

impl From<&AttachmentConfig> for AttachmentLimits {
    fn from(cfg: &AttachmentConfig) -> Self {
        Self {
            enabled: cfg.enabled,
            max_file_bytes: cfg.max_file_bytes,
            max_files_per_turn: cfg.max_files_per_turn,
        }
    }
}

/// 按路径扩展名路由出的附件大类。非图片 / PDF 一律按 UTF-8 文本尝试。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    Pdf,
    Text,
}

/// 已规格化的媒体附件（图片或 PDF），`data` 为 base64。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedMedia {
    pub media_type: String,
    pub data: String,
    pub kind: AttachmentKind,
    pub source_name: String,
}

/// `read_attachment` 的结果：文本直接展开，媒体走 base64 块。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAttachment {
    Text { content: String },
    Media(NormalizedMedia),
}

#[derive(Debug, thiserror::Error)]
pub enum AttachmentError {
    #[error("附件路径不存在或不可读取: {path}: {source}")]
    Metadata {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("附件路径是目录而不是文件: {0}")]
    IsDirectory(String),
    #[error("附件不是常规文件: {0}")]
    NotFile(String),
    #[error("附件过大: {name} 为 {actual} bytes，超过上限 {limit} bytes")]
    TooLarge {
        name: String,
        actual: u64,
        limit: u64,
    },
    #[error("图片内容校验失败（{source_name}）: {reason}")]
    InvalidImage { source_name: String, reason: String },
    #[error("PDF 内容校验失败（{source_name}）: {reason}")]
    InvalidPdf { source_name: String, reason: String },
    #[error("读取附件失败: {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("附件不是有效 UTF-8 文本（仅支持图片 png/jpg/jpeg/gif/webp、PDF 与 UTF-8 文本）: {0}")]
    NotUtf8(String),
    #[error("单轮附件数量超限: {actual} 个，最多 {limit} 个")]
    TooManyFiles { actual: usize, limit: usize },
    #[error("受保护的 memory 文件必须通过 memory 工具访问: {0}")]
    ProtectedMemoryPath(String),
    #[error("附件处理任务失败: {0}")]
    Task(String),
    #[error("附件功能已禁用")]
    Disabled,
}

/// tool_result JSON 中携带媒体附件的保留键，由 turn loop 剥离后转为独立内容块。
pub const FILE_READ_MEDIA_KEY: &str = "media";

impl NormalizedMedia {
    pub fn to_json(&self) -> Value {
        json!({
            "media_type": self.media_type,
            "data": self.data,
            "kind": match self.kind {
                AttachmentKind::Image => "image",
                AttachmentKind::Pdf => "pdf",
                AttachmentKind::Text => "text",
            },
            "source_name": self.source_name,
        })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let kind = match value.get("kind")?.as_str()? {
            "image" => AttachmentKind::Image,
            "pdf" => AttachmentKind::Pdf,
            _ => return None,
        };
        Some(Self {
            media_type: value.get("media_type")?.as_str()?.to_string(),
            data: value.get("data")?.as_str()?.to_string(),
            kind,
            source_name: value
                .get("source_name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        })
    }
}

/// 按扩展名路由附件大类。内容级校验在读取时进行，最终 media_type 以实际内容为准。
pub fn attachment_kind_for_path(path: &Path) -> AttachmentKind {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("png") | Some("jpg") | Some("jpeg") | Some("gif") | Some("webp") => {
            AttachmentKind::Image
        }
        Some("pdf") => AttachmentKind::Pdf,
        _ => AttachmentKind::Text,
    }
}

/// 受保护的 memory 文件（`memories/MEMORY.md` / `memories/USER.md`）只允许
/// memory 工具访问，附件与 file_read 一律拒绝。判定是词法级的（先消解 `..`）。
pub fn is_protected_memory_path(path: &Path) -> bool {
    let normalized = normalize_path_lexically(path);
    let is_memory_file = normalized
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "MEMORY.md" || name == "USER.md");
    let under_memories_dir = normalized
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "memories");
    is_memory_file && under_memories_dir
}

fn normalize_path_lexically(path: &Path) -> std::path::PathBuf {
    use std::path::Component;
    let mut out = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(component.as_os_str()),
            Component::Normal(part) => out.push(part),
        }
    }
    out
}

/// 读取任意路径附件：按扩展名路由到图片 / PDF / UTF-8 文本，并完成内容校验。
pub async fn read_attachment(
    path: &Path,
    limits: &AttachmentLimits,
) -> Result<ReadAttachment, AttachmentError> {
    ensure_attachments_enabled(limits)?;
    match attachment_kind_for_path(path) {
        AttachmentKind::Image => Ok(ReadAttachment::Media(
            read_image_attachment(path, limits).await?,
        )),
        AttachmentKind::Pdf => Ok(ReadAttachment::Media(
            read_document_attachment(path, limits).await?,
        )),
        AttachmentKind::Text => Ok(ReadAttachment::Text {
            content: read_text_attachment(path, limits).await?,
        }),
    }
}

/// 读取并规格化图片附件（校验真实内容 + 超限重采样）。
pub async fn read_image_attachment(
    path: &Path,
    limits: &AttachmentLimits,
) -> Result<NormalizedMedia, AttachmentError> {
    ensure_attachments_enabled(limits)?;
    let bytes = read_validated_bytes(path, limits).await?;
    normalize_image_attachment_with_limits(bytes, &source_name_of(path), limits).await
}

/// 读取并校验 PDF 附件（仅基础校验，不做内容抽取）。
pub async fn read_document_attachment(
    path: &Path,
    limits: &AttachmentLimits,
) -> Result<NormalizedMedia, AttachmentError> {
    ensure_attachments_enabled(limits)?;
    let bytes = read_validated_bytes(path, limits).await?;
    normalize_pdf_attachment(&bytes, &source_name_of(path))
}

/// 读取 UTF-8 文本附件。
pub async fn read_text_attachment(
    path: &Path,
    limits: &AttachmentLimits,
) -> Result<String, AttachmentError> {
    ensure_attachments_enabled(limits)?;
    let bytes = read_validated_bytes(path, limits).await?;
    String::from_utf8(bytes).map_err(|_| AttachmentError::NotUtf8(path.display().to_string()))
}

fn ensure_attachments_enabled(limits: &AttachmentLimits) -> Result<(), AttachmentError> {
    if limits.enabled {
        Ok(())
    } else {
        Err(AttachmentError::Disabled)
    }
}

/// 规格化图片：用魔数推断真实格式、完整解码校验、最长边超过
/// [`IMAGE_MAX_EDGE_PX`] 时等比缩小并重编码（jpeg 保持 jpeg，其余统一 png）。
pub async fn normalize_image_attachment(
    bytes: Vec<u8>,
    source_name: &str,
) -> Result<NormalizedMedia, AttachmentError> {
    let source_name = source_name.to_string();
    // 图片解码 / 重采样是 CPU 密集操作，放进 spawn_blocking 避免阻塞异步运行时。
    tokio::task::spawn_blocking(move || normalize_image_attachment_sync(bytes, source_name))
        .await
        .map_err(|e| AttachmentError::Task(e.to_string()))?
}

/// 规格化图片，并确保重编码后的输出仍不超过单文件大小限制。
pub async fn normalize_image_attachment_with_limits(
    bytes: Vec<u8>,
    source_name: &str,
    limits: &AttachmentLimits,
) -> Result<NormalizedMedia, AttachmentError> {
    let source_name = source_name.to_string();
    let max_file_bytes = limits.max_file_bytes;
    tokio::task::spawn_blocking(move || {
        normalize_image_attachment_sync_with_limit(bytes, source_name, max_file_bytes)
    })
    .await
    .map_err(|e| AttachmentError::Task(e.to_string()))?
}

/// [`normalize_image_attachment`] 的同步版本，供已身处 `spawn_blocking` 的调用方使用。
pub fn normalize_image_attachment_sync(
    bytes: Vec<u8>,
    source_name: String,
) -> Result<NormalizedMedia, AttachmentError> {
    normalize_image_attachment_sync_with_limit(bytes, source_name, u64::MAX)
}

/// 同步规格化图片，并对重编码后的 bytes 做大小上限兜底。
pub fn normalize_image_attachment_sync_with_limit(
    bytes: Vec<u8>,
    source_name: String,
    max_file_bytes: u64,
) -> Result<NormalizedMedia, AttachmentError> {
    let invalid = |reason: String| AttachmentError::InvalidImage {
        source_name: source_name.clone(),
        reason,
    };
    let format =
        image::guess_format(&bytes).map_err(|e| invalid(format!("无法识别图片格式: {e}")))?;
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Gif | ImageFormat::WebP
    ) {
        return Err(invalid(format!("不支持的图片格式: {format:?}")));
    }
    let decoded = ImageReader::with_format(Cursor::new(&bytes), format)
        .decode()
        .map_err(|e| invalid(format!("图片解码失败: {e}")))?;

    if decoded.width().max(decoded.height()) <= IMAGE_MAX_EDGE_PX {
        // 尺寸已合规：透传原始字节，media_type 以真实内容为准（而非扩展名）。
        return Ok(NormalizedMedia {
            media_type: image_media_type(format).to_string(),
            data: BASE64_STANDARD.encode(&bytes),
            kind: AttachmentKind::Image,
            source_name,
        });
    }

    let mut last_size = 0u64;
    for edge in IMAGE_DOWNSAMPLE_EDGE_PX {
        let resized = decoded.resize(*edge, *edge, image::imageops::FilterType::Triangle);
        let (out, media_type) = encode_resized_image(&resized, format, &invalid)?;
        let actual = u64::try_from(out.len()).unwrap_or(u64::MAX);
        last_size = actual;
        if actual <= max_file_bytes {
            return Ok(NormalizedMedia {
                media_type: media_type.to_string(),
                data: BASE64_STANDARD.encode(&out),
                kind: AttachmentKind::Image,
                source_name,
            });
        }
    }
    Err(AttachmentError::TooLarge {
        name: source_name,
        actual: last_size,
        limit: max_file_bytes,
    })
}

fn encode_resized_image(
    resized: &image::DynamicImage,
    format: ImageFormat,
    invalid: &impl Fn(String) -> AttachmentError,
) -> Result<(Vec<u8>, &'static str), AttachmentError> {
    let mut out = Vec::new();
    if format == ImageFormat::Jpeg {
        // jpeg 不支持 alpha 通道，重编码前转 RGB。
        image::DynamicImage::ImageRgb8(resized.to_rgb8())
            .write_to(&mut Cursor::new(&mut out), ImageFormat::Jpeg)
            .map_err(|e| invalid(format!("图片重编码失败: {e}")))?;
        return Ok((out, "image/jpeg"));
    }
    resized
        .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
        .map_err(|e| invalid(format!("图片重编码失败: {e}")))?;
    Ok((out, "image/png"))
}

/// 规格化 PDF：仅校验 `%PDF-` 魔数，整本 base64 透传，不做抽取 / OCR。
pub fn normalize_pdf_attachment(
    bytes: &[u8],
    source_name: &str,
) -> Result<NormalizedMedia, AttachmentError> {
    if !bytes.starts_with(b"%PDF-") {
        return Err(AttachmentError::InvalidPdf {
            source_name: source_name.to_string(),
            reason: "文件头不是 %PDF-，不是合法 PDF".into(),
        });
    }
    Ok(NormalizedMedia {
        media_type: "application/pdf".to_string(),
        data: BASE64_STANDARD.encode(bytes),
        kind: AttachmentKind::Pdf,
        source_name: source_name.to_string(),
    })
}

/// 元数据校验（存在 / 是文件 / 不超限 / 非受保护 memory 文件）后读取整个文件。
async fn read_validated_bytes(
    path: &Path,
    limits: &AttachmentLimits,
) -> Result<Vec<u8>, AttachmentError> {
    if is_protected_memory_path(path) {
        return Err(AttachmentError::ProtectedMemoryPath(
            path.display().to_string(),
        ));
    }
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|source| AttachmentError::Metadata {
            path: path.display().to_string(),
            source,
        })?;
    if metadata.is_dir() {
        return Err(AttachmentError::IsDirectory(path.display().to_string()));
    }
    if !metadata.is_file() {
        return Err(AttachmentError::NotFile(path.display().to_string()));
    }
    if metadata.len() > limits.max_file_bytes {
        return Err(AttachmentError::TooLarge {
            name: path.display().to_string(),
            actual: metadata.len(),
            limit: limits.max_file_bytes,
        });
    }
    tokio::fs::read(path)
        .await
        .map_err(|source| AttachmentError::Read {
            path: path.display().to_string(),
            source,
        })
}

fn source_name_of(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("attachment")
        .to_string()
}

fn image_media_type(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Png => "image/png",
        ImageFormat::Jpeg => "image/jpeg",
        ImageFormat::Gif => "image/gif",
        ImageFormat::WebP => "image/webp",
        // 调用方已先过滤白名单格式，此分支只是兜底
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_png_bytes() -> Vec<u8> {
        let img = image::DynamicImage::ImageRgb8(image::RgbImage::new(2, 2));
        let mut out = Vec::new();
        img.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .expect("编码测试 PNG 不应失败");
        out
    }

    fn wide_png_bytes(width: u32) -> Vec<u8> {
        let img = image::DynamicImage::ImageRgb8(image::RgbImage::new(width, 1));
        let mut out = Vec::new();
        img.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
            .expect("编码测试 PNG 不应失败");
        out
    }

    #[test]
    fn kind_routing_uses_extension() {
        assert_eq!(
            attachment_kind_for_path(Path::new("a.PNG")),
            AttachmentKind::Image
        );
        assert_eq!(
            attachment_kind_for_path(Path::new("b.pdf")),
            AttachmentKind::Pdf
        );
        assert_eq!(
            attachment_kind_for_path(Path::new("c.rs")),
            AttachmentKind::Text
        );
        assert_eq!(
            attachment_kind_for_path(Path::new("Makefile")),
            AttachmentKind::Text
        );
    }

    #[tokio::test]
    async fn normalize_image_detects_media_type_from_content_not_extension() {
        let media = normalize_image_attachment(tiny_png_bytes(), "fake.jpg")
            .await
            .unwrap();
        assert_eq!(media.media_type, "image/png");
        assert_eq!(media.kind, AttachmentKind::Image);
        assert_eq!(media.source_name, "fake.jpg");
    }

    #[tokio::test]
    async fn normalize_image_rejects_non_image_bytes() {
        let err = normalize_image_attachment(b"not an image".to_vec(), "bad.png")
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::InvalidImage { .. }));
    }

    #[tokio::test]
    async fn normalize_image_downscales_to_max_edge() {
        let media = normalize_image_attachment(wide_png_bytes(IMAGE_MAX_EDGE_PX + 100), "wide.png")
            .await
            .unwrap();
        let bytes = BASE64_STANDARD.decode(&media.data).unwrap();
        let decoded = image::load_from_memory(&bytes).unwrap();
        assert!(decoded.width() <= IMAGE_MAX_EDGE_PX);
        assert_eq!(media.media_type, "image/png");
    }

    #[tokio::test]
    async fn normalize_image_rejects_when_downsampled_output_exceeds_limit() {
        let limits = AttachmentLimits {
            enabled: true,
            max_file_bytes: 1,
            max_files_per_turn: 5,
        };
        let err = normalize_image_attachment_with_limits(
            wide_png_bytes(IMAGE_MAX_EDGE_PX + 100),
            "wide.png",
            &limits,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AttachmentError::TooLarge { .. }));
    }

    #[test]
    fn normalize_pdf_requires_magic_header() {
        assert!(normalize_pdf_attachment(b"%PDF-1.7 fake", "a.pdf").is_ok());
        assert!(matches!(
            normalize_pdf_attachment(b"plain text", "a.pdf"),
            Err(AttachmentError::InvalidPdf { .. })
        ));
    }

    #[tokio::test]
    async fn read_attachment_rejects_directory_and_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let limits = AttachmentLimits::default();
        assert!(matches!(
            read_attachment(dir.path(), &limits).await,
            Err(AttachmentError::IsDirectory(_))
        ));
        assert!(matches!(
            read_attachment(&dir.path().join("missing.txt"), &limits).await,
            Err(AttachmentError::Metadata { .. })
        ));
    }

    #[tokio::test]
    async fn read_attachment_enforces_max_file_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.txt");
        tokio::fs::write(&path, vec![b'a'; 32]).await.unwrap();
        let limits = AttachmentLimits {
            enabled: true,
            max_file_bytes: 16,
            max_files_per_turn: 5,
        };
        assert!(matches!(
            read_attachment(&path, &limits).await,
            Err(AttachmentError::TooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn read_attachment_rejects_when_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        tokio::fs::write(&path, b"hello").await.unwrap();
        let limits = AttachmentLimits {
            enabled: false,
            ..AttachmentLimits::default()
        };

        assert!(matches!(
            read_attachment(&path, &limits).await,
            Err(AttachmentError::Disabled)
        ));
    }

    #[tokio::test]
    async fn read_attachment_reads_utf8_text_and_rejects_binary() {
        let dir = tempfile::tempdir().unwrap();
        let text_path = dir.path().join("note.md");
        tokio::fs::write(&text_path, "你好 attachment")
            .await
            .unwrap();
        let limits = AttachmentLimits::default();
        assert_eq!(
            read_attachment(&text_path, &limits).await.unwrap(),
            ReadAttachment::Text {
                content: "你好 attachment".into()
            }
        );

        let bin_path = dir.path().join("blob.bin");
        tokio::fs::write(&bin_path, [0xff, 0xfe, 0x00, 0x80])
            .await
            .unwrap();
        assert!(matches!(
            read_attachment(&bin_path, &limits).await,
            Err(AttachmentError::NotUtf8(_))
        ));
    }

    #[test]
    fn normalized_media_json_round_trip() {
        let media = NormalizedMedia {
            media_type: "application/pdf".into(),
            data: "QUJD".into(),
            kind: AttachmentKind::Pdf,
            source_name: "brief.pdf".into(),
        };
        let back = NormalizedMedia::from_json(&media.to_json()).unwrap();
        assert_eq!(media, back);
    }

    #[tokio::test]
    async fn protected_memory_paths_are_rejected() {
        assert!(is_protected_memory_path(Path::new(
            "agent-a/memories/MEMORY.md"
        )));
        assert!(is_protected_memory_path(Path::new(
            "agent-a/memories/sub/../USER.md"
        )));
        assert!(!is_protected_memory_path(Path::new("agent-a/MEMORY.md")));

        let dir = tempfile::tempdir().unwrap();
        let memories = dir.path().join("memories");
        std::fs::create_dir_all(&memories).unwrap();
        let path = memories.join("MEMORY.md");
        std::fs::write(&path, "secret").unwrap();
        assert!(matches!(
            read_attachment(&path, &AttachmentLimits::default()).await,
            Err(AttachmentError::ProtectedMemoryPath(_))
        ));
    }
}
