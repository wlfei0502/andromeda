# Agent Middleware Framework 设计

日期：2026-09-19  
状态：已批准  
范围：把「主模型调用前」的横切逻辑收成可插拔中间件链；本里程碑 **行为不变** 地迁入现有上下文摘要。  
前置：[2026-09-19-context-summarization-design.md](./2026-09-19-context-summarization-design.md)

## 目标

- 编排只负责：steer → **middleware `before_llm` 链** → 主 LLM → tool / follow-up。
- 以后堆 memory / todos / guards 时，只需新增 `middleware/*.rs` 并挂到链上，不必再改 loop 骨架。
- 本迭代：**仅重构**；摘要语义、SSE、overflow、测试期望与 M2 一致。

## 非目标

- DeerFlow 全套钩子（`after_model` jump、`wrap_model_call` 等）——预留扩展点，本版不实现。
- 动态插件加载 / 宏注册。
- 实现 Memory / Plan / Guards 业务（可留目录注释；不写 noop 占位文件，避免死代码）。
- 改变摘要写入形态（仍写回 `context`，不引入独立 `summary_text` 字段）。

---

## §1 目录与模块

```text
src/agent/
  middleware/
    mod.rs           # AgentMiddleware trait、MwCtx、MwEffect、run_before_llm
    summarize.rs     # SummarizeMiddleware（调用现有 maybe_summarize 逻辑）
  summarize.rs       # 保留纯函数：estimate_tokens / split_context / maybe_summarize
                     # （或迁入 middleware/summarize/internals；优先少动测试路径）
  orchestrator.rs    # 调用 run_before_llm，应用 MwEffect
  mod.rs
```

**决议**：核心摘要算法仍放在 `agent/summarize.rs`（单测路径稳定）；`middleware/summarize.rs` 只做 `AgentMiddleware` 适配。

---

## §2 Trait 与上下文

```rust
pub struct MwCtx<'a> {
    pub run_id: &'a str,
    pub context: &'a mut Vec<WireMessage>,
    pub tools: &'a [ToolDef],
    pub pending_tool: Option<&'a PendingTool>,
    pub llm: &'a dyn LlmPort,
}

pub struct MwEffect {
    pub events: Vec<SseEvent>,
    pub checkpoint: bool,  // true → orchestrator 刷 Running checkpoint
}

impl MwEffect {
    pub fn none() -> Self { Self { events: vec![], checkpoint: false } }
}

pub enum MwAction {
    Continue(MwEffect),
}

#[async_trait]
pub trait AgentMiddleware: Send + Sync {
    fn name(&self) -> &'static str;
    async fn before_llm(
        &self,
        ctx: &mut MwCtx<'_>,
    ) -> Result<MwAction, OrchestratorError>;
}
```

**原则**

1. Middleware **不**持有 `RunHandle`；副作用只通过 `MwEffect` 上报，由 orchestrator 统一 `emit` + `checkpoint`。
2. 第一版只有 `Continue`；失败用 `Err(OrchestratorError)`（如 `ContextOverflow`）。
3. 链顺序：配置/装配时的 `Vec` 顺序即执行顺序。默认：`[SummarizeMiddleware]`。

```rust
pub async fn run_before_llm(
    chain: &[Arc<dyn AgentMiddleware>],
    ctx: &mut MwCtx<'_>,
) -> Result<MwEffect, OrchestratorError> {
    let mut merged = MwEffect::none();
    for mw in chain {
        match mw.before_llm(ctx).await? {
            MwAction::Continue(effect) => {
                merged.events.extend(effect.events);
                merged.checkpoint |= effect.checkpoint;
            }
        }
    }
    Ok(merged)
}
```

---

## §3 SummarizeMiddleware

```rust
pub struct SummarizeMiddleware {
    pub config: ContextConfig,
}

// before_llm:
//   match maybe_summarize(ctx.context.clone(), &self.config, ctx.llm, ctx.pending_tool)
//     Unchanged → Continue(none)
//     Summarized → *ctx.context = new; Continue(events=[ContextSummarized], checkpoint=true)
//     ContextOverflow → Err(OrchestratorError::ContextOverflow)
```

删除 orchestrator 内的 `apply_summarize_if_needed`；改为：

```text
drain_steer / cancel 检查
→ run_before_llm(chain, …)
→ apply effects (emit + optional checkpoint)
→ stream_llm
```

---

## §4 装配

- `AppState` 增加 `middlewares: Arc<[Arc<dyn AgentMiddleware>]>`（或 `Vec`）。
- `main`：`vec![Arc::new(SummarizeMiddleware { config: config.context.clone() })]`。
- 测试：`AppState` / `run_agent` 传入含 `SummarizeMiddleware` 的链，或接受 `middlewares` 参数；默认测试用带默认 `ContextConfig` 的 Summarize（高阈值 ≈ 不触发），集成测继续压低阈值。
- `run_agent*` 签名：增加 `middlewares: Arc<[Arc<dyn AgentMiddleware>]>`（或与 `context_cfg` 合并——**决议**：去掉单独传入的 `ContextConfig`，改由 SummarizeMiddleware 持有；其它测试若需关摘要，传空链或极大阈值的 middleware）。

**兼容决议**：`run_agent` 不再单独收 `context_cfg`；摘要配置只在 middleware 上。HTTP / main / 测试全部改接线。

---

## §5 扩展预留（不实现）

| 未来中间件 | 钩子 | 说明 |
|------------|------|------|
| Memory | `before_llm` 注入；日后可加 `after_llm` | 检索 / 入队 |
| Todos | `before_llm` 提醒；server tool 另议 | 对齐 DeerFlow TodoMiddleware 的一部分 |
| Guards | `before_llm` 检查轮次 | 超限 `Err` |

本版 **不** 增加 `after_llm` trait 方法；需要时在 `AgentMiddleware` 上加 `async fn after_llm` 默认空实现即可，无需破链。

---

## §6 测试与出口

1. 现有 `agent::summarize` 单测全部绿（算法文件尽量不动）。
2. `tests/context_summarize.rs`、`orchestrator`、`http_api`、checkpoint 测全部绿。
3. 无行为回归：超阈仍发 `context.summarized`；overflow 仍 `context_overflow`。
4. 文档：`docs/api-client.md` 可补一句「server 可在 LLM 前跑 middleware 链；当前含 summarization」——可选，非阻塞。

## 出口标准

- `src/agent/middleware/` 存在且含 trait + Summarize 适配。
- Orchestrator 经 `run_before_llm` 调摘要，无内联 `apply_summarize_if_needed`。
- 全量 `cargo test` 通过。
