# LH-M4 Guards 设计

日期：2026-09-21  
状态：按已批准的长任务规格 §5 实现  
范围：把长跑护栏做成服务端配置，超限时结束该 run。

依据：[2026-09-18-long-horizon-design.md](./2026-09-18-long-horizon-design.md) §5、§12。

## 行为

每个 `run_id` 独立计数。配置在 `[guards]`，进程级，不按请求覆盖。`0` 表示关闭该条护栏。

| 键 | 默认 | 检查点 | 结束 |
|----|------|--------|------|
| `max_llm_rounds` | 200 | 每次主 LLM 调用**之前**；成功结束后 `llm_rounds + 1` | `run.finished` `reason=guard_llm_rounds` |
| `max_run_wall_secs` | 7200 | 每次主 LLM 之前，以及等待客户端 tool 之前；自 `guards.started_at`（unix 秒） | `reason=guard_timeout` |
| `max_follow_up_rounds` | 8 | 外循环注入 follow-up 之前（替换硬编码 `8`） | 保持现有 `error` / `code=follow_up_limit` |
| `max_noop_llm_rounds` | 5 | 一次 LLM 无 tool、且本轮没有 steer 续跑时 | `reason=guard_noop` |

护栏结束：发 `run.finished`（不发 `error`），checkpoint `status=failed`，`finish_reason` 记下 reason。不要静默继续。

`tool_timeout_secs` 不变。

## 空转

一轮计入连续空转，当且仅当：本轮没有 tool call，且助手文本与**上一轮**助手文本高度相似。

- 第一轮只记下文本，连续计数为 0。
- 有 tool，或本轮结束后 drain 到 steer：连续计数清零。
- todos 变化只会伴随 `write_todos` tool call，因此走「有 tool」分支，不计入空转。
- 高度相似：折叠空白并小写后相等，或较短串被较长串包含且长度 ≥ 80%，或字符 bigram Jaccard ≥ 0.85。
- `max_noop_llm_rounds = N` 表示连续 N 次「与上一轮相似的无进展回复」后停止（不含第一句基线）。

## 续跑

冷恢复 / `waiting_tool` 续跑沿用 checkpoint 里的 `GuardsSnapshot`（含 `llm_rounds`、`follow_up_rounds`、`started_at`、空转计数）。新 run 用 `GuardsSnapshot::new_now()`。

## 非目标

- 客户端按 run 覆盖护栏
- 子代理配额（LH-M5）
- 改 tool 超时语义
