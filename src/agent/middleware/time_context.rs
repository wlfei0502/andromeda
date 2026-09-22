//! Inject wall-clock "now" so the model does not invent a training-cutoff date.

use async_trait::async_trait;
use chrono::{Datelike, Local, Timelike, Weekday};

use super::{AgentMiddleware, MwAction, MwCtx, MwEffect};
use crate::agent::OrchestratorError;
use crate::protocol::{Role, WireMessage};

pub const TIME_CONTEXT_PREFIX: &str = "[current time]";

pub struct TimeContextMiddleware;

fn weekday_zh(w: Weekday) -> &'static str {
    match w {
        Weekday::Mon => "星期一",
        Weekday::Tue => "星期二",
        Weekday::Wed => "星期三",
        Weekday::Thu => "星期四",
        Weekday::Fri => "星期五",
        Weekday::Sat => "星期六",
        Weekday::Sun => "星期日",
    }
}

fn time_context_message() -> WireMessage {
    let now = Local::now();
    let content = format!(
        "{TIME_CONTEXT_PREFIX} 当前本地时间：{}年{}月{}日 {} {:02}:{:02}（偏移 {}）。\
         回答「今天 / 现在 / 星期几」等问题时必须以本条时间为准，不要使用训练数据或对话中过期的日期。",
        now.year(),
        now.month(),
        now.day(),
        weekday_zh(now.weekday()),
        now.hour(),
        now.minute(),
        now.format("%:z"),
    );
    WireMessage {
        role: Role::System,
        content,
        tool_call_id: None,
        name: None,
        tool_calls: None,
        reasoning_content: None,
    }
}

/// Insert or refresh the `[current time]` system message at the front of context.
pub fn ensure_time_context(context: &mut Vec<WireMessage>) {
    let msg = time_context_message();
    if let Some(existing) = context
        .iter_mut()
        .find(|m| m.role == Role::System && m.content.starts_with(TIME_CONTEXT_PREFIX))
    {
        existing.content = msg.content;
        return;
    }
    context.insert(0, msg);
}

#[async_trait]
impl AgentMiddleware for TimeContextMiddleware {
    fn name(&self) -> &'static str {
        "time_context"
    }

    async fn before_llm(&self, ctx: &mut MwCtx<'_>) -> Result<MwAction, OrchestratorError> {
        ensure_time_context(ctx.context);
        Ok(MwAction::Continue(MwEffect::none()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockLlm;
    use crate::protocol::{Role, ToolDef};
    use std::sync::Arc;

    #[test]
    fn inserts_then_refreshes_prefix() {
        let mut ctx = vec![WireMessage {
            role: Role::User,
            content: "今天星期几".into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        }];
        ensure_time_context(&mut ctx);
        assert_eq!(ctx.len(), 2);
        assert!(ctx[0].content.starts_with(TIME_CONTEXT_PREFIX));
        let first = ctx[0].content.clone();
        ensure_time_context(&mut ctx);
        assert_eq!(ctx.len(), 2);
        assert!(ctx[0].content.starts_with(TIME_CONTEXT_PREFIX));
        assert_eq!(ctx[0].content, first);
    }

    #[tokio::test]
    async fn middleware_injects_before_llm() {
        let llm = MockLlm::script(vec![]);
        let mut context = vec![WireMessage {
            role: Role::User,
            content: "今天星期几".into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            reasoning_content: None,
        }];
        let tools = &[] as &[ToolDef];
        let mut ctx = MwCtx {
            run_id: "run-1",
            context: &mut context,
            tools,
            pending_tool: None,
            llm: &llm,
        };
        let mw = TimeContextMiddleware;
        match mw.before_llm(&mut ctx).await.unwrap() {
            MwAction::Continue(effect) => {
                assert!(!effect.checkpoint);
                assert!(effect.events.is_empty());
            }
        }
        assert!(context[0].content.starts_with(TIME_CONTEXT_PREFIX));
        let _ = Arc::new(mw);
    }
}
