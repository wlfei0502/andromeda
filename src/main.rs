use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;

use futures::StreamExt;
use liter_llm::{ClientConfigBuilder, DefaultClient};
use serde::Deserialize;

use andromeda::{
    AgentContext, AgentEvent, AgentMessage, ContentPart, LiterBackend, agent_loop, echo_tool_def,
};

const CONFIG_PATH: &str = "config.toml";

#[derive(Debug, Deserialize)]
struct AppConfig {
    api_key: String,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default = "default_model")]
    model: String,
}

fn default_model() -> String {
    "gpt-4o-mini".into()
}

fn load_config(path: &Path) -> Result<AppConfig, String> {
    let text = fs::read_to_string(path).map_err(|err| {
        format!(
            "failed to read {CONFIG_PATH}: {err}\n\
             Copy config.example.toml to config.toml and fill in api_key."
        )
    })?;
    let config: AppConfig = toml::from_str(&text)
        .map_err(|err| format!("failed to parse {CONFIG_PATH}: {err}"))?;
    if config.api_key.trim().is_empty() {
        return Err(format!("{CONFIG_PATH}: api_key is required"));
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> ExitCode {
    let config = match load_config(Path::new(CONFIG_PATH)) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };

    let mut builder = ClientConfigBuilder::new(config.api_key);
    if let Some(url) = config.base_url {
        builder = builder.base_url(url);
    }
    let client_config = builder.build();
    let client = match DefaultClient::new(client_config, Some(config.model.as_str())) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("failed to create LLM client: {err}");
            return ExitCode::from(1);
        }
    };

    let backend = Arc::new(LiterBackend::new(client, config.model.clone()).with_echo());
    let ctx = AgentContext {
        messages: vec![],
        tools: vec![echo_tool_def()],
    };
    let prompts = vec![AgentMessage::User {
        content: "Reply with a short greeting. You may use the echo tool.".into(),
    }];

    eprintln!(
        "calling model `{}` (waiting for network; default timeout ~60s)...",
        config.model
    );

    let mut stream = match agent_loop(prompts, ctx, backend, None) {
        Ok(s) => s,
        Err(err) => {
            eprintln!("agent_loop failed: {err}");
            return ExitCode::from(1);
        }
    };
    let handle = stream.result_handle();

    let mut printed = 0usize;
    let mut had_text = false;
    let mut saw_error = false;
    while let Some(ev) = stream.next().await {
        match ev {
            AgentEvent::MessageUpdate { message } => {
                if let Some(text) = assistant_text(&message) {
                    if text.len() > printed {
                        print!("{}", &text[printed..]);
                        let _ = io::stdout().flush();
                        printed = text.len();
                        had_text = true;
                    }
                }
            }
            AgentEvent::MessageEnd { message } => {
                if matches!(
                    message,
                    AgentMessage::Assistant {
                        stop_reason: andromeda::StopReason::Error
                            | andromeda::StopReason::Aborted,
                        ..
                    }
                ) {
                    saw_error = true;
                }
                if matches!(message, AgentMessage::Assistant { .. }) && printed > 0 {
                    println!();
                }
                printed = 0;
            }
            AgentEvent::AgentEnd { .. } => {}
            _ => {}
        }
    }

    let _ = handle.await;
    if saw_error {
        eprintln!(
            "model call failed (check base_url reachability / api_key).\n\
             tip: curl -I --connect-timeout 5 <your-base-url-host>"
        );
        return ExitCode::from(1);
    }
    if !had_text {
        eprintln!("finished with no assistant text printed.");
    }
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
