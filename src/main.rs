use std::io::{self, Write};
use std::process::ExitCode;
use std::sync::Arc;

use futures::StreamExt;
use liter_llm::{ClientConfigBuilder, DefaultClient};

use andromeda::{
    AgentContext, AgentEvent, AgentMessage, ContentPart, LiterBackend, agent_loop, echo_tool_def,
};

#[tokio::main]
async fn main() -> ExitCode {
    let Ok(api_key) = std::env::var("ANDROMEDA_API_KEY") else {
        eprintln!(
            "ANDROMEDA_API_KEY is required.\n\
             Set ANDROMEDA_API_KEY (and optionally ANDROMEDA_BASE_URL, ANDROMEDA_MODEL), then re-run."
        );
        return ExitCode::from(1);
    };

    let base_url = std::env::var("ANDROMEDA_BASE_URL").ok();
    let model = std::env::var("ANDROMEDA_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());

    let mut builder = ClientConfigBuilder::new(api_key);
    if let Some(url) = base_url {
        builder = builder.base_url(url);
    }
    let config = builder.build();
    let client = match DefaultClient::new(config, Some(model.as_str())) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("failed to create LLM client: {err}");
            return ExitCode::from(1);
        }
    };

    let backend = Arc::new(LiterBackend::new(client, model).with_echo());
    let ctx = AgentContext {
        messages: vec![],
        tools: vec![echo_tool_def()],
    };
    let prompts = vec![AgentMessage::User {
        content: "Reply with a short greeting. You may use the echo tool.".into(),
    }];

    let mut stream = match agent_loop(prompts, ctx, backend, None) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("agent_loop failed: {err}");
            return ExitCode::from(1);
        }
    };
    let handle = stream.result_handle();

    let mut printed = 0usize;
    while let Some(ev) = stream.next().await {
        match ev {
            AgentEvent::MessageUpdate { message } => {
                if let Some(text) = assistant_text(&message) {
                    if text.len() > printed {
                        print!("{}", &text[printed..]);
                        let _ = io::stdout().flush();
                        printed = text.len();
                    }
                }
            }
            AgentEvent::MessageEnd { message } => {
                if matches!(message, AgentMessage::Assistant { .. }) && printed > 0 {
                    println!();
                }
                printed = 0;
            }
            _ => {}
        }
    }

    let _ = handle.await;
    ExitCode::SUCCESS
}

fn assistant_text(message: &AgentMessage) -> Option<String> {
    match message {
        AgentMessage::Assistant { parts, .. } => {
            let text: String = parts
                .iter()
                .filter_map(|p| match p {
                    ContentPart::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            Some(text)
        }
        _ => None,
    }
}
