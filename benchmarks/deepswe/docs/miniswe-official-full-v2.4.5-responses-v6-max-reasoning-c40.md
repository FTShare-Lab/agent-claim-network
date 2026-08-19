# MiniSWE 2.4.5 Responses：显式 reasoning=max 的 40 并发复现

本 profile 使用 113 个 DeepSWE task、每题 2 个 attempt、总计 226 条试验与 40 并发。请求走 Responses 协议，并显式发送 `reasoning.effort=max` 与 `max_output_tokens=65536`。

凭据和 endpoint 只通过运行时环境变量传入，不要写入 YAML、文档或 Git。

启动前审计会确认上述字段位于 Responses 实际请求的 `model_kwargs`，并拒绝历史上无效的外层 `reasoning_effort` 与 `max_tokens` 字段。

```sh
export MINISWE_DEEPSWE_ROOT=/absolute/path/to/deep-swe
export MINISWE_PIER_ROOT=/absolute/path/to/pier
export MINISWE_RUN_ROOT=/absolute/path/to/private-run-root
bash benchmarks/deepswe/scripts/apply-pier-overlay.sh
bash benchmarks/deepswe/scripts/run-miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40.sh
```

DeepSWE 冻结为 `435ee89ec2f2e2289f33b0da4f992f0b7b7266b9`，Pier 冻结为 `0daf53d3599e58c4506cf0bcff5e12c77dc282d2`。overlay hash 为 `7cb51ffacd2807a76d70c0ae22e051f840c3d499866233b328af676429a8b154`。结果不能宣称为未使用该 overlay 的原始供应方榜单复跑。

首个完成 trajectory 落盘后运行 wire 验证；任一响应未返回 `reasoning.effort=max` 或 `max_output_tokens=65536` 时，本轮不能用于协议比较。

```sh
bash benchmarks/deepswe/scripts/verify-miniswe-official-full-v2.4.5-responses-v6-max-reasoning-c40-wire.sh
```
