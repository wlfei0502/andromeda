# Context Summarization（LH-P0b / LH-M2）设计

日期：2026-09-19  
状态：已批准  
范围：在 Cloud Agent Server 上实现 **运行时上下文摘要**，对应长任务规格的 LH-P0b / 里程碑 LH-M2。  
前置文档：

- [2026-09-18-cloud-agent-sse-design.md](./2026-09-18-cloud-agent-sse-design.md)
- [2026-09-18-long-horizon-design.md](./2026-09-18-long-horizon-design.md) §3

## 目标

当 run 的 `context` 估算体积超过配置阈值时，在 **下一次主模型调用前** 自动压缩中间历史，使多轮短任务与长任务都不因 tool 结果堆积而触顶、拖慢或烧光配额。

## 非目标（M2）

- 真实 tokenizer（tiktoken 等）；M2 仅用字符启发式
- 独立 `summarizer_model` 配置（固定与主 `model` / 同一 `LlmPort`）
- 独立 `summarize_enabled` 开关（靠把阈值调极大等价关闭）
- 摘要失败后的「二次再压」循环
- 新的 Wire `Role`；长期记忆 / 跨 run 记忆
- Plan Mode、Guards、子代理（仍属后续里程碑）

## 相对长任务规格的修订

父规格早期把摘要字段写在 `[long_horizon]` 下。本设计修订为：

| 项 | 父规格（旧） | 本设计（已定） |
|----|--------|----------------|
| 持久化配置节 | `[long_horizon]` | **`[persist]`**（代码仍接受旧别名） |
| 摘要配置节 | 混在 `[long_horizon]` | **`[context]`**（与 persist 开关解耦） |
| 启用条件 | 隐含随 LH | **无摘要开关**；仅看阈值 |
| Token 估算 | token 或字符启发式 | **固定 `chars / 4`** |
| 摘要模型 | 可选 `summarizer_model` | **M2 不做**；始终主 model |
| 模块边界 | Summarizer 组件 | **独立** `src/agent/summarize.rs`，编排只调用 |

父规格其余策略（切段、SSE、硬上限失败、刷盘）保持一致。

---

## §1 架构

```text
run_agent 循环
  │
  ├─ maybe_summarize(context, cfg, llm, pending_tool?)
  │     ├─ estimate_tokens < threshold → 原样返回
  │     ├─ split prefix / middle / suffix
  │     ├─ middle 空 → 原样返回
  │     ├─ LLM 摘要 middle → 一条 system 摘要消息
  │     ├─ 拼回 context；失败则保留原文
  │     └─ 若仍 ≥ max_context_tokens → Err(context_overflow)
  │
  ├─（成功且已改写）emit context.summarized + checkpoint（若 persist）
  └─ 主模型 llm.complete(context, tools)
```

**原则**

1. 摘要是 **运行时策略**，不是模型自发 tool。
2. 摘要 LLM 调用 **不计入** follow-up 轮次 /（日后的）`max_llm_rounds`。
3. 与 `persist.enabled`、单次 run 的 `options.persist` **无关**；persist 只影响摘要后是否刷盘。

---

## §2 配置

```toml
[context]
summarize_threshold_tokens = 80000
keep_last_messages = 24
max_context_tokens = 120000
```

| 字段 | 默认 | 含义 |
|------|------|------|
| `summarize_threshold_tokens` | `80000` | 估算 ≥ 此值则尝试摘要 |
| `keep_last_messages` | `24` | 尾部至少保留的消息条数（tool 链可再扩展） |
| `max_context_tokens` | `120000` | 硬上限；摘要后（或无法摘要时）仍 ≥ 则结束 run |

省略 `[context]` 时使用上表默认值。  
将 `summarize_threshold_tokens` 设为极大（或 ≥ `max_context_tokens` 且实际跑不到）即可等价关闭摘要。

`AppConfig` 增加 `context: ContextConfig`；`config.example.toml` 同步示例。

---

## §3 Token 估算

```text
estimate_tokens(messages) = ceil( sum over messages of
    utf8_len(content)
  + utf8_len(name?) 
  + utf8_len(serde_json of tool_calls?)
) / 4
```

不引入外部 tokenizer。阈值按「估算 token」配置，与启发式一致即可调参。

---

## §4 切段与写回

### 4.1 切分

1. **前缀**：开头连续的 `role=system` 消息全部保留。  
2. **尾部**：从末尾取 `keep_last_messages` 条。若切断未完成 tool 链（assistant 带 `tool_calls`，对应 `tool` 结果尚未齐），则把整条链向前并入尾部。  
3. **强制保留**：与当前 `pending_tool`（若有）相关的消息；尚未被主模型消费的 steer 用户消息——若落在中间，并入尾部。  
4. **中间**：其余消息。中间为空则 **跳过摘要**。

### 4.2 摘要调用

- 同一 `LlmPort`，`tools = []`。  
- 输入：固定 system 提示 + 中间段的可读转写（role / name / tool_call_id / content / tool_calls 摘要）。  
- System 提示要求输出结构化短摘要：目标、已完成步骤、关键事实、未决事项。  
- 收齐非流式文本（可内部 drain stream）；**不**向客户端转发摘要 delta。

### 4.3 写回形态

```text
prefix + [ WireMessage { role: System, content: "[conversation summary]\n..." } ] + suffix
```

- 固定前缀 `[conversation summary]`，便于识别，避免后续切段把摘要当普通人设误处理（切前缀时：仅保留 **原始** 开头 system；已有的 summary 消息视为可被再次摘要的中间内容，除非它已落在尾部）。  
- **规则细化**：前缀 = 开头连续 system，且 content **不以** `[conversation summary]` 开头。这样旧摘要可进入中间被再压缩。

---

## §5 SSE 与错误

### 5.1 `context.summarized`

```json
{
  "type": "context.summarized",
  "run_id": "...",
  "before_tokens": 95000,
  "after_tokens": 12000,
  "kept_prefix": 1,
  "kept_suffix": 24
}
```

`kept_suffix` 为实际尾部条数（含 tool 链扩展后）。

### 5.2 `context_overflow`

发已有 `error` 事件：

```json
{
  "type": "error",
  "run_id": "...",
  "message": "context exceeded max_context_tokens after summarization attempt",
  "code": "context_overflow"
}
```

然后结束 run（与现有 error 终态路径一致）。**不**再发成功的 `run.finished`，除非现有编排对 error 已有统一收尾约定——实现须与当前 `OrchestratorError` → SSE 行为对齐。

### 5.3 失败矩阵

| 情况 | 行为 |
|------|------|
| 估算 < threshold | 不摘要 |
| 中间为空 | 不摘要；若 ≥ max → overflow |
| 摘要 LLM 失败 / 空文本 | 打日志，保留原 context；若 ≥ max → overflow |
| 摘要成功但仍 ≥ max | overflow（M2 不二次摘要） |
| 摘要成功且 < max | 替换 context，发 `context.summarized`，persist 则刷盘 |

---

## §6 编排集成

- 挂点：每次主 `llm` 调用 **之前**（含首轮与 tool 返回后的续轮）。  
- `run_agent` / HTTP 装配传入 `ContextConfig` + 同一 `llm`。  
- 摘要成功改写后：`emit context.summarized` → `checkpoint`（若 persist）→ 再主调用。  
- 新增 `OrchestratorError` 变体或映射：`ContextOverflow` → SSE `code=context_overflow`。

---

## §7 测试策略

以 `MockLlm` 为主，不依赖真实 MaaS。

| # | 用例 | 期望 |
|---|------|------|
| 1 | 短 context | 不变；无 `context.summarized`；主模型收到原文 |
| 2 | 超阈、中间可切 | 中间替换为带 `[conversation summary]` 的 system；有 SSE；主模型看到压缩后 context |
| 3 | 尾部含未完成 tool 链 | 链完整留在尾部 |
| 4 | 摘要 LLM 失败 + 超硬上限 | `context_overflow` |
| 5 | threshold 极大 | 等价关闭 |
| 6 | 旧 summary 再超阈 | 旧 summary 可进入中间被新 summary 替换 |

---

## §8 文档与客户端

- 更新 `docs/api-client.md`：新事件 `context.summarized`、错误码 `context_overflow`、`[context]` 配置说明。  
- 桌面仓：可忽略该事件（调试用）；必须能处理 `error` + `context_overflow`（结束 UI / 提示）。

---

## §9 出口标准（LH-M2）

1. `[context]` 可配置且有默认值。  
2. 超阈时调用前摘要；`context.summarized` 可观测。  
3. 硬上限失败路径可测。  
4. 与 `[persist]` 开关解耦：关闭持久化时摘要仍可按阈值工作。  
5. `api-client.md` 已同步。
