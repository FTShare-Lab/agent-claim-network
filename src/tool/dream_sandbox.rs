//! Dream 命令的工作区读取隔离；白名单仍使用既有 Bash 分类器，进程仍由统一 runner 管理。
use super::file::lexical_normalize_path;
use super::*;

pub(super) const COMMAND_PATH: &str =
    "/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin";

async fn command_available(name: &str, search_path: &str) -> bool {
    if matches!(name, "cd" | "pwd" | "true" | "false") {
        return true;
    }
    for directory in std::env::split_paths(search_path) {
        if let Ok(metadata) = fs::metadata(directory.join(name)).await {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
                    return true;
                }
            }
            #[cfg(not(unix))]
            if metadata.is_file() {
                return true;
            }
        }
    }
    false
}

pub(super) fn protected_evidence_path(path: &Path) -> bool {
    path.components().any(|component| {
        let name = component.as_os_str().to_string_lossy().to_ascii_lowercase();
        matches!(
            name.as_str(),
            "memory.md"
                | "user.md"
                | "export_env.sh"
                | "credentials"
                | "credentials.json"
                | ".ssh"
                | ".aws"
                | ".gnupg"
                | ".acn"
        ) || name == ".env"
            || name.starts_with(".env.")
    })
}

impl ToolRegistry {
    pub(crate) async fn dream_command_environment(&self) -> Value {
        let mut available = Vec::new();
        let mut unavailable = Vec::new();
        for name in [
            "ls", "rg", "grep", "cat", "head", "tail", "wc", "stat", "file", "cd", "pwd", "true",
            "false",
        ] {
            if command_available(name, COMMAND_PATH).await {
                available.push(name);
            } else {
                unavailable.push(name);
            }
        }
        json!({"available_commands":available,"unavailable_commands":unavailable,
            "note":"Executable lookup in the restricted PATH, not a guarantee that every option or file is accessible. Do not call unavailable commands. Use ls/grep to locate files, file_read for versioned evidence, or read_trace for provenance. A command failure is not evidence about a Claim."})
    }

    async fn dream_private_root(&self) -> Result<Option<PathBuf>, ToolError> {
        let Some(access) = &self.knowledge else {
            return Ok(None);
        };
        let path = access.private_root();
        match fs::canonicalize(&path).await {
            Ok(root) => Ok(Some(root)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Some(path)),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) async fn validate_dream_path(&self, path: &Path) -> Result<PathBuf, ToolError> {
        let canonical = fs::canonicalize(path).await?;
        let workspace = fs::canonicalize(&self.workspace_root).await?;
        let private = self.dream_private_root().await?;
        if !canonical.starts_with(&workspace)
            || protected_evidence_path(&canonical)
            || private.as_ref().is_some_and(|p| canonical.starts_with(p))
        {
            return Err(ToolError::InvalidArgs("Dream only reads workspace evidence; private data and other agents are unavailable".into()));
        }
        Ok(canonical)
    }

    pub(super) async fn validate_dream_command(&self, input: &Value) -> Result<(), ToolError> {
        if !self.is_concurrency_safe("code_run", input) {
            return Err(ToolError::InvalidArgs(format!(
                "command is outside the existing read-only allowlist. {}",
                knowledge::DREAM_COMMAND_GUIDANCE
            )));
        }
        let args: CodeRunArgs = serde_json::from_value(input.clone())
            .map_err(|e| ToolError::InvalidArgs(e.to_string()))?;
        let commands = concurrency::safe_bash_commands(&args.script)
            .ok_or_else(|| ToolError::InvalidArgs("invalid read-only command".into()))?;
        for name in commands {
            if !command_available(&name, COMMAND_PATH).await {
                return Err(ToolError::InvalidArgs(format!(
                    "Dream command {name} is unavailable in the restricted PATH; no command was executed. Use ls/grep to locate files, file_read to read known files, or read_trace to locate provenance. This is a tool availability failure, not missing evidence in the project."
                )));
            }
        }
        let cwd = self
            .validate_dream_path(&resolve_tool_path(
                &self.workspace_root,
                args.cwd.as_deref().unwrap_or("."),
            ))
            .await?;
        let workspace = fs::canonicalize(&self.workspace_root).await?;
        let literals = concurrency::safe_bash_literals(&args.script)
            .ok_or_else(|| ToolError::InvalidArgs("invalid read-only command".into()))?;
        for value in literals {
            if value.starts_with('~') || protected_evidence_path(Path::new(&value)) {
                return Err(ToolError::InvalidArgs(
                    "private paths are unavailable in Dream commands".into(),
                ));
            }
            let candidate = resolve_tool_path(&cwd, &value);
            if (Path::new(&value).is_absolute() || value.split('/').any(|part| part == ".."))
                && !lexical_normalize_path(&candidate).starts_with(&workspace)
            {
                return Err(ToolError::InvalidArgs(
                    "Dream command path leaves workspace".into(),
                ));
            }
            if fs::try_exists(&candidate).await? {
                self.validate_dream_path(&candidate).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn dream_command_spec(
        &self,
        script: &str,
        cwd: &Path,
    ) -> Result<(String, Vec<String>), ToolError> {
        let workspace = fs::canonicalize(&self.workspace_root).await?;
        let cwd = self.validate_dream_path(cwd).await?;
        #[cfg(target_os = "macos")]
        {
            let quoted = |path: &Path| {
                serde_json::to_string(&path.to_string_lossy())
                    .map_err(|e| ToolError::InvalidArgs(e.to_string()))
            };
            let root = quoted(&workspace)?;
            let private = self.dream_private_root().await?;
            let mut profile = format!(
                r#"(version 1)
(deny default)
(allow process*)
(allow sysctl-read)
(allow file-read-metadata)
(allow file-read* (literal "/"))
(allow system-mac-syscall (mac-policy-name "vnguard"))
(allow system-mac-syscall (require-all (mac-policy-name "Sandbox") (mac-syscall-number 67)))
(allow file-map-executable (subpath "/bin") (subpath "/usr") (subpath "/System") (subpath "/Library/Apple") (subpath "/opt/homebrew") (subpath "/usr/local"))
(allow file-read* (subpath {root}) (subpath "/bin") (subpath "/usr") (subpath "/System") (subpath "/Library/Apple") (subpath "/opt/homebrew/bin") (subpath "/opt/homebrew/lib") (subpath "/opt/homebrew/Cellar") (subpath "/usr/local/bin") (subpath "/usr/local/lib") (literal "/dev/null") (literal "/dev/urandom"))
(allow file-write-data (literal "/dev/null"))
(deny file-read* (regex #"/([Mm][Ee][Mm][Oo][Rr][Yy]|[Uu][Ss][Ee][Rr])[.][Mm][Dd]$") (regex #"/([.]env([.][^/]*)?|export_env[.]sh|credentials([.]json)?|[.]ssh|[.]aws|[.]gnupg|[.]acn)(/|$)"))
"#
            );
            if let Some(private) = private {
                profile.push_str(&format!(
                    "(deny file-read* (subpath {}))\n",
                    quoted(&private)?
                ));
            }
            let _ = cwd;
            Ok((
                "/usr/bin/sandbox-exec".into(),
                vec![
                    "-p".into(),
                    profile,
                    "/bin/bash".into(),
                    "--noprofile".into(),
                    "--norc".into(),
                    "-c".into(),
                    script.into(),
                ],
            ))
        }
        #[cfg(target_os = "linux")]
        {
            // 系统缺少隔离器或禁用 user namespaces 时 fail closed，模型仍可使用分页读取工具。
            let mut args = vec![
                "--die-with-parent".into(),
                "--unshare-all".into(),
                "--new-session".into(),
            ];
            for root in ["/usr", "/bin", "/lib", "/lib64"] {
                if fs::try_exists(root).await? {
                    args.extend(["--ro-bind".into(), root.into(), root.into()]);
                }
            }
            args.extend([
                "--proc".into(),
                "/proc".into(),
                "--dev".into(),
                "/dev".into(),
                "--tmpfs".into(),
                "/tmp".into(),
                "--ro-bind".into(),
                workspace.to_string_lossy().into_owned(),
                workspace.to_string_lossy().into_owned(),
            ]);
            let mut stack = vec![workspace.clone()];
            let mut count = 0usize;
            let private_root = self.dream_private_root().await?;
            while let Some(dir) = stack.pop() {
                let mut entries = fs::read_dir(&dir).await?;
                while let Some(entry) = entries.next_entry().await? {
                    count += 1;
                    if count > 100_000 {
                        return Err(ToolError::InvalidArgs(
                            "Dream workspace isolation limit reached; use file_read".into(),
                        ));
                    }
                    let path = entry.path();
                    let kind = entry.file_type().await?;
                    let private = private_root
                        .as_ref()
                        .is_some_and(|root| path.starts_with(root));
                    if protected_evidence_path(&path) || private {
                        if kind.is_dir() {
                            args.extend(["--tmpfs".into(), path.to_string_lossy().into_owned()]);
                        } else {
                            args.extend([
                                "--ro-bind".into(),
                                "/dev/null".into(),
                                path.to_string_lossy().into_owned(),
                            ]);
                        }
                    } else if kind.is_dir() {
                        stack.push(path);
                    }
                }
            }
            args.extend([
                "--chdir".into(),
                cwd.to_string_lossy().into_owned(),
                "--".into(),
                "/bin/bash".into(),
                "--noprofile".into(),
                "--norc".into(),
                "-c".into(),
                script.into(),
            ]);
            Ok(("bwrap".into(), args))
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (script, cwd, workspace);
            Err(ToolError::InvalidArgs(
                "Dream command isolation is unavailable; use file_read".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dream_command_lookup_does_not_inherit_host_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        assert!(!command_available("rg", path).await);
        assert!(command_available("pwd", path).await);
        // 参数中出现 rg 不应被识别为需要执行 rg。
        assert_eq!(
            concurrency::safe_bash_commands("grep -n rg file | head -n 3"),
            Some(vec!["grep".into(), "head".into()])
        );
        assert_eq!(
            concurrency::safe_bash_commands("ls -la; rg --files | head -n 120"),
            Some(vec!["head".into(), "ls".into(), "rg".into()])
        );
        assert!(concurrency::safe_bash_commands("ls $(pwd)").is_none());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let binary = dir.path().join("rg");
            fs::write(&binary, "fixture").await.unwrap();
            fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o644))
                .await
                .unwrap();
            assert!(!command_available("rg", path).await);
            fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
                .await
                .unwrap();
            assert!(command_available("rg", path).await);
        }
    }
}
