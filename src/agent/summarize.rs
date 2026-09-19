//! Context token estimate, split helpers, and optional LLM summarization.

use futures::StreamExt;

use crate::config::ContextConfig;
use crate::llm::{LlmChunk, LlmPort};
use crate::protocol::{Role, WireMessage};
use crate::store::PendingTool;

pub const SUMMARY_PREFIX: &str = "[conversation summary]";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSplit {
    pub prefix: Vec<WireMessage>,
    pub middle: Vec<WireMessage>,
    pub suffix: Vec<WireMessage>,
}

pub fn estimate_tokens(messages: &[WireMessage]) -> u64 {
    let mut bytes = 0usize;
    for m in messages {
        bytes += m.content.len();
        if let Some(name) = &m.name {
            bytes += name.len();
        }
        if let Some(tcs) = &m.tool_calls {
            bytes += serde_json::to_string(tcs).map(|s| s.len()).unwrap_or(0);
        }
    }
    bytes.div_ceil(4) as u64
}

/// Returns true when the proposed cut would leave tool-related messages in `middle`.
///
/// Besides open chains (assistant in suffix missing a matching tool result), any
/// assistant with `tool_calls` still in the middle slice also forces suffix expansion —
/// conservative vs. only checking incomplete chains at the suffix boundary (brief step 3).
fn cutting_splits_open_tool_chain(messages: &[WireMessage], prefix_end: usize, suffix_start: usize) -> bool {
    let len = messages.len();
    for i in prefix_end..suffix_start {
        if messages[i].role == Role::Assistant && messages[i].tool_calls.is_some() {
            return true;
        }
    }
    for i in suffix_start..len {
        let m = &messages[i];
        if m.role != Role::Assistant {
            continue;
        }
        let Some(tcs) = &m.tool_calls else {
            continue;
        };
        for tc in tcs {
            let has_matching_tool_in_suffix = messages[i + 1..].iter().enumerate().any(|(offset, msg)| {
                let j = i + 1 + offset;
                j >= suffix_start
                    && msg.role == Role::Tool
                    && msg.tool_call_id.as_deref() == Some(tc.id.as_str())
            });
            if !has_matching_tool_in_suffix {
                return true;
            }
        }
    }
    false
}

fn middle_contains_pending(messages: &[WireMessage], start: usize, end: usize, pending: &PendingTool) -> bool {
    messages[start..end].iter().any(|m| {
        m.tool_call_id.as_deref() == Some(pending.tool_call_id.as_str())
            || m.tool_calls.as_ref().is_some_and(|tcs| {
                tcs.iter()
                    .any(|tc| tc.id == pending.tool_call_id)
            })
    })
}

pub fn split_context(
    messages: &[WireMessage],
    keep_last: usize,
    pending: Option<&PendingTool>,
) -> ContextSplit {
    let len = messages.len();

    let mut prefix_end = 0;
    while prefix_end < len {
        let m = &messages[prefix_end];
        if m.role == Role::System && !m.content.starts_with(SUMMARY_PREFIX) {
            prefix_end += 1;
        } else {
            break;
        }
    }

    if len <= prefix_end {
        return ContextSplit {
            prefix: messages[..prefix_end].to_vec(),
            middle: vec![],
            suffix: vec![],
        };
    }

    let mut suffix_start = len.saturating_sub(keep_last.max(1));
    suffix_start = suffix_start.max(prefix_end);

    while suffix_start > prefix_end && cutting_splits_open_tool_chain(messages, prefix_end, suffix_start) {
        suffix_start -= 1;
    }

    if let Some(pending) = pending {
        while suffix_start > prefix_end
            && middle_contains_pending(messages, prefix_end, suffix_start, pending)
        {
            suffix_start -= 1;
        }
    }

    ContextSplit {
        prefix: messages[..prefix_end].to_vec(),
        middle: messages[prefix_end..suffix_start].to_vec(),
        suffix: messages[suffix_start..].to_vec(),
    }
}

const SUMMARIZER_SYSTEM_PROMPT: &str = "\
Summarize the conversation segment for continuation. Be concise. \
Use exactly these section headers on their own lines: Goal, Done, Facts, Open.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummarizeOutcome {
    Unchanged,
    Summarized {
        context: Vec<WireMessage>,
        before_tokens: u64,
        after_tokens: u64,
        kept_prefix: usize,
        kept_suffix: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummarizeError {
    ContextOverflow {
        before_tokens: u64,
        after_tokens: u64,
    },
}

fn overflow_on_context(before: u64, messages: &[WireMessage], max: u64) -> Result<(), SummarizeError> {
    let after = estimate_tokens(messages);
    if after >= max {
        Err(SummarizeError::ContextOverflow {
            before_tokens: before,
            after_tokens: after,
        })
    } else {
        Ok(())
    }
}

fn format_middle_transcript(middle: &[WireMessage]) -> String {
    let mut lines = Vec::with_capacity(middle.len());
    for m in middle {
        let mut header = format!("[{}]", role_label(&m.role));
        if let Some(name) = &m.name {
            header.push_str(&format!(" name={name}"));
        }
        if let Some(id) = &m.tool_call_id {
            header.push_str(&format!(" tool_call_id={id}"));
        }
        lines.push(format!("{header}\n{}", m.content));
        if let Some(tcs) = &m.tool_calls {
            if let Ok(json) = serde_json::to_string(tcs) {
                lines.push(format!("  tool_calls: {json}"));
            }
        }
    }
    lines.join("\n\n")
}

fn role_label(role: &Role) -> &'static str {
    match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "tool",
    }
}

fn build_summarizer_prompt(middle: &[WireMessage]) -> Vec<WireMessage> {
    vec![
        WireMessage {
            role: Role::System,
            content: SUMMARIZER_SYSTEM_PROMPT.into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        },
        WireMessage {
            role: Role::User,
            content: format_middle_transcript(middle),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        },
    ]
}

async fn complete_text(llm: &dyn LlmPort, messages: &[WireMessage]) -> Result<String, String> {
    let mut stream = llm.stream(messages, &[]).await?;
    let mut content = None;
    while let Some(chunk) = stream.next().await {
        match chunk? {
            LlmChunk::TextDelta(_) => {}
            LlmChunk::Completed { content: c, .. } => content = Some(c),
        }
    }
    content
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "empty summary".into())
}

pub async fn maybe_summarize(
    context: Vec<WireMessage>,
    cfg: &ContextConfig,
    llm: &dyn LlmPort,
    pending: Option<&PendingTool>,
) -> Result<SummarizeOutcome, SummarizeError> {
    let before = estimate_tokens(&context);
    let max = cfg.max_context_tokens;

    if before < cfg.summarize_threshold_tokens {
        overflow_on_context(before, &context, max)?;
        return Ok(SummarizeOutcome::Unchanged);
    }

    let split = split_context(&context, cfg.keep_last_messages, pending);
    if split.middle.is_empty() {
        overflow_on_context(before, &context, max)?;
        return Ok(SummarizeOutcome::Unchanged);
    }

    let prompt = build_summarizer_prompt(&split.middle);
    let summary_text = match complete_text(llm, &prompt).await {
        Ok(text) => text,
        Err(err) => {
            tracing::warn!(error = %err, "context summarization failed; keeping original context");
            overflow_on_context(before, &context, max)?;
            return Ok(SummarizeOutcome::Unchanged);
        }
    };

    let summary_msg = WireMessage {
        role: Role::System,
        content: format!("{SUMMARY_PREFIX}\n{summary_text}"),
        tool_call_id: None,
        name: None,
        tool_calls: None,
    };

    let kept_prefix = split.prefix.len();
    let kept_suffix = split.suffix.len();
    let mut new_context = split.prefix;
    new_context.push(summary_msg);
    new_context.extend(split.suffix);

    let after = estimate_tokens(&new_context);
    if after >= max {
        return Err(SummarizeError::ContextOverflow {
            before_tokens: before,
            after_tokens: after,
        });
    }

    Ok(SummarizeOutcome::Summarized {
        context: new_context,
        before_tokens: before,
        after_tokens: after,
        kept_prefix,
        kept_suffix,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Role, ToolCallWire, WireMessage};
    use serde_json::json;

    fn msg(role: Role, content: &str) -> WireMessage {
        WireMessage {
            role,
            content: content.into(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
        }
    }

    #[test]
    fn estimate_tokens_chars_div_4() {
        let m = msg(Role::User, "abcd"); // 4 bytes → 1 token
        assert_eq!(estimate_tokens(&[m]), 1);
    }

    #[test]
    fn split_keeps_leading_real_system_and_tail() {
        let messages = vec![
            msg(Role::System, "you are helpful"),
            msg(Role::User, "u1"),
            msg(Role::Assistant, "a1"),
            msg(Role::User, "u2"),
            msg(Role::Assistant, "a2"),
        ];
        let split = split_context(&messages, 2, None);
        assert_eq!(split.prefix.len(), 1);
        assert_eq!(split.suffix.len(), 2);
        assert_eq!(split.middle.len(), 2);
        assert_eq!(split.middle[0].content, "u1");
    }

    #[test]
    fn split_excludes_summary_system_from_prefix() {
        let messages = vec![
            msg(Role::System, "you are helpful"),
            msg(Role::System, &format!("{SUMMARY_PREFIX}\nold")),
            msg(Role::User, "u1"),
            msg(Role::Assistant, "a1"),
        ];
        let split = split_context(&messages, 2, None);
        assert_eq!(split.prefix.len(), 1);
        assert!(split.middle[0].content.starts_with(SUMMARY_PREFIX));
    }

    #[test]
    fn split_extends_suffix_for_open_tool_chain() {
        let _messages = vec![
            msg(Role::User, "start"),
            WireMessage {
                role: Role::Assistant,
                content: "".into(),
                tool_call_id: None,
                name: None,
                tool_calls: Some(vec![ToolCallWire {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }]),
            },
            // no tool result yet — keep_last=1 would otherwise cut the assistant
            msg(Role::User, "later"),
        ];
        // Force a small keep_last so extension matters: use messages without the trailing user
        let open = vec![
            msg(Role::User, "start"),
            msg(Role::User, "pad"),
            WireMessage {
                role: Role::Assistant,
                content: "".into(),
                tool_call_id: None,
                name: None,
                tool_calls: Some(vec![ToolCallWire {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }]),
            },
        ];
        let split = split_context(&open, 1, None);
        assert!(
            split.suffix.iter().any(|m| m.tool_calls.is_some()),
            "open tool assistant must stay in suffix"
        );
        assert!(split.suffix.len() >= 2);
    }

    #[test]
    fn split_extends_suffix_when_middle_has_assistant_tool_calls() {
        let messages = vec![
            msg(Role::User, "start"),
            WireMessage {
                role: Role::Assistant,
                content: "".into(),
                tool_call_id: None,
                name: None,
                tool_calls: Some(vec![ToolCallWire {
                    id: "c1".into(),
                    name: "echo".into(),
                    arguments: json!({}),
                }]),
            },
            WireMessage {
                role: Role::Tool,
                content: "ok".into(),
                tool_call_id: Some("c1".into()),
                name: Some("echo".into()),
                tool_calls: None,
            },
            msg(Role::User, "tail"),
        ];
        let split = split_context(&messages, 1, None);
        assert!(
            !split
                .middle
                .iter()
                .any(|m| m.role == Role::Assistant && m.tool_calls.is_some()),
            "completed chain assistant must not remain in middle"
        );
        assert!(
            split.suffix.iter().any(|m| m.tool_calls.is_some()),
            "assistant with tool_calls must be pulled into suffix"
        );
    }

    #[test]
    fn split_extends_suffix_for_pending_tool_in_middle() {
        use crate::store::PendingTool;

        // Orphan tool row (no assistant+tool_calls in slice): open-chain rule does not
        // expand, but keep_last=1 would leave the pending id in middle without pending.
        let messages = vec![
            msg(Role::User, "old"),
            msg(Role::User, "older"),
            WireMessage {
                role: Role::Tool,
                content: "result".into(),
                tool_call_id: Some("pending-1".into()),
                name: Some("run".into()),
                tool_calls: None,
            },
            msg(Role::User, "latest"),
        ];
        let without = split_context(&messages, 1, None);
        assert!(
            without
                .middle
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("pending-1")),
            "fixture: without pending expansion the tool result would sit in middle"
        );

        let pending = PendingTool {
            tool_call_id: "pending-1".into(),
            name: "run".into(),
            arguments: json!({}),
        };
        let with_pending = split_context(&messages, 1, Some(&pending));
        assert!(
            with_pending
                .suffix
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("pending-1")),
            "pending tool result must be force-kept in suffix"
        );
        assert!(
            !with_pending
                .middle
                .iter()
                .any(|m| m.tool_call_id.as_deref() == Some("pending-1")),
            "pending-related messages must not remain in middle"
        );
    }

    use crate::config::ContextConfig;
    use crate::llm::{MockLlm, MockTurn};

    #[tokio::test]
    async fn below_threshold_unchanged() {
        let cfg = ContextConfig {
            summarize_threshold_tokens: 10_000,
            keep_last_messages: 2,
            max_context_tokens: 20_000,
        };
        let ctx = vec![msg(Role::User, "hi")];
        let llm = MockLlm::script(vec![]);
        let out = maybe_summarize(ctx.clone(), &cfg, &llm, None).await.unwrap();
        assert!(matches!(out, SummarizeOutcome::Unchanged));
        assert!(llm.recorded_contexts().is_empty());
    }

    #[tokio::test]
    async fn over_threshold_replaces_middle() {
        let cfg = ContextConfig {
            summarize_threshold_tokens: 1,
            keep_last_messages: 2,
            max_context_tokens: 100_000,
        };
        let mut ctx = vec![msg(Role::System, "persona")];
        for i in 0..6 {
            ctx.push(msg(Role::User, &format!("user-{i}-{}", "x".repeat(20))));
            ctx.push(msg(Role::Assistant, &format!("asst-{i}")));
        }
        let llm = MockLlm::script(vec![MockTurn::TextOnly {
            content: "Goal: test\nDone: steps\nFacts: f\nOpen: none".into(),
            deltas: vec![],
        }]);
        let out = maybe_summarize(ctx, &cfg, &llm, None).await.unwrap();
        let SummarizeOutcome::Summarized { context, .. } = out else {
            panic!("expected summarized");
        };
        assert!(context.iter().any(|m| m.content.starts_with(SUMMARY_PREFIX)));
        assert_eq!(llm.recorded_contexts().len(), 1);
    }

    #[tokio::test]
    async fn summarize_fail_over_max_overflows() {
        let cfg = ContextConfig {
            summarize_threshold_tokens: 1,
            keep_last_messages: 2,
            max_context_tokens: 1,
        };
        let mut ctx = vec![msg(Role::System, "persona")];
        for i in 0..6 {
            ctx.push(msg(Role::User, &format!("user-{i}-{}", "x".repeat(40))));
        }
        let llm = MockLlm::script(vec![MockTurn::Fail {
            message: "boom".into(),
        }]);
        let err = maybe_summarize(ctx, &cfg, &llm, None).await.unwrap_err();
        assert!(matches!(err, SummarizeError::ContextOverflow { .. }));
    }
}
