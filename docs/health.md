# M / R 健康检查与团队鉴权探测

Maintainer 和 Router 都公开 `GET /health` 与 `POST /health`，无需管理员登录或团队 Key。旧的无请求体 GET 保持 HTTP 200，响应由空体变为 JSON。探针只检查服务存活与可选团队凭据，不承诺业务资源可访问，也不检查模型、检索等依赖是否就绪。

## 请求与响应

无请求体 `GET /health`（或空请求体 POST、`{}`、`{"data":{}}`）：

```json
{"status":"ok","team_auth_enabled":true}
```

`team_auth_enabled` 在 Router 上对应 `[router.auth.team].enabled`，在 Maintainer 上对应 `[maintainer.auth.team].enabled`，与管理员鉴权无关。

提供团队凭据时，推荐 `POST /health` 和 `Content-Type: application/json`，复用团队信封；GET 携带同样 JSON 请求体也支持。不要把 Key 放在 URL 查询参数中。

```json
{
  "auth": {"agent_id":"health-agent","acn_key":"<team-key>"},
  "data": {}
}
```

有效凭据返回 HTTP 200：

```json
{"status":"ok","team_auth_enabled":true,"auth_passed":true}
```

错误 Key、Agent ID 与 Key 不匹配、Key 已撤销返回 HTTP 200：

```json
{"status":"ok","team_auth_enabled":true,"auth_passed":false}
```

鉴权关闭时沿用团队鉴权的放行语义，提交完整信封会得到 `team_auth_enabled:false, auth_passed:true`；这表示不要求校验凭据。未提供 `auth`（包括 `auth:null`）时省略 `auth_passed`。该字段仅表示团队凭据校验结果，具体业务端点仍执行自己的身份与对象权限检查；管理员 Basic / Cookie 无法代替团队信封。

探测使用当前 Key 台账，不修改业务鉴权缓存；台账读取失败时，带凭据探测返回 `auth_passed:false`，无凭据探针仍返回 200。响应不包含 Key、密码、Key hash 或解析错误细节，并禁止缓存。无效 JSON、缺少凭据字段或非法信封结构返回 400；请求体超出框架限制仍返回 413。这些是请求格式错误，与凭据校验失败的 HTTP 200 分开处理。

## 自动验收实际进程

在仓库根目录运行。Rust 验收程序复用仓库现有依赖，会启动真实 M/R、通过管理员 API 创建临时团队 Key，并检查撤销前后结果；默认结束后清理临时数据和进程，不读取已有团队配置。

```bash
if [[ -f export_env.sh ]]; then source export_env.sh; fi
cargo build --bins --examples
npm --prefix frontend/maintainer-workbench run build
target/debug/examples/health_auth_smoke
target/debug/examples/health_auth_smoke --router-auth-disabled
target/debug/examples/health_auth_smoke --maintainer-auth-disabled
target/debug/examples/health_auth_smoke --router-auth-disabled --maintainer-auth-disabled
```

每次运行均检查：无信封、空信封、有效/撤销 Key、GET 带信封、错误 Key、身份不匹配、无效 JSON。所有完整信封都返回 200，字段随实际服务的开关和凭据状态变化。

## 手工 curl 验收

第一终端启动并保持运行，记录打印的临时目录；准备就绪时出现 `READY`：

```bash
HEALTH_TEST_DIR="$(mktemp -d /tmp/acn-health-manual.XXXXXX)"
printf '%s\n' "$HEALTH_TEST_DIR"
target/debug/examples/health_auth_smoke --directory "$HEALTH_TEST_DIR" --serve
```

第二终端将下面路径换成第一终端打印的路径。测试凭据只存在权限受限的临时文件中；请求命令不打印 Key。

```bash
HEALTH_TEST_DIR=/tmp/acn-health-manual.XXXXXX
source "$HEALTH_TEST_DIR/endpoints.sh"

curl -sS -i "$M_URL/health"
curl -sS -i "$R_URL/health"

for url in "$M_URL" "$R_URL"; do
  curl -sS -i -H 'Content-Type: application/json' --data-binary @"$HEALTH_TEST_DIR/valid.json" "$url/health"
  curl -sS -i -H 'Content-Type: application/json' --data-binary @"$HEALTH_TEST_DIR/wrong-key.json" "$url/health"
  curl -sS -i -H 'Content-Type: application/json' --data-binary @"$HEALTH_TEST_DIR/wrong-agent.json" "$url/health"
done
```

预期：无凭据响应省略 `auth_passed`；有效 Key 为 `true`；错误 Key / 身份不匹配为 `false`；以上全部 HTTP 200，两个服务的 `team_auth_enabled` 都为 `true`。

撤销 Key 后，不重启 M/R，再探测：

```bash
curl -sS -i -X POST -H @"$HEALTH_TEST_DIR/admin-header.txt" "$M_URL/api/team-auth/keys/$HEALTH_KEY_ID/revoke"
for url in "$M_URL" "$R_URL"; do
  curl -sS -i -H 'Content-Type: application/json' --data-binary @"$HEALTH_TEST_DIR/valid.json" "$url/health"
done
```

预期撤销 API 返回 200，两次 health 也返回 200 且 `auth_passed:false`。第一终端 Ctrl-C 会停止两个进程；确认结束后删除该临时目录。要手工观察不同开关，在第一终端的启动命令中追加 `--router-auth-disabled` 或 `--maintainer-auth-disabled`，使用一个新临时目录重新启动。
