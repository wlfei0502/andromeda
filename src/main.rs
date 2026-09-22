use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use andromeda::agent::{default_summarize_chain, policy_from_name};
use andromeda::api::{AppState, router};
use andromeda::llm::LiterAdapter;
use andromeda::runtime::RunRegistry;
use andromeda::store::{LocalFsRunStore, RunStore};
use tower_http::trace::TraceLayer;

const CONFIG_PATH: &str = "config.toml";

#[tokio::main]
async fn main() -> ExitCode {
    let config = match andromeda::AppConfig::load(Path::new(CONFIG_PATH)) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };

    let filter = tracing_subscriber::EnvFilter::try_new(&config.log_level).unwrap_or_else(|err| {
        eprintln!(
            "invalid log_level {:?}: {err}; falling back to info",
            config.log_level
        );
        tracing_subscriber::EnvFilter::new("info")
    });
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let instance_id = if config.persist.instance_id.trim().is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        config.persist.instance_id.clone()
    };

    tracing::info!(
        model = %config.model,
        listen = %config.listen,
        tool_timeout_secs = config.tool_timeout_secs,
        follow_up_policy = %config.follow_up_policy,
        log_level = %config.log_level,
        base_url = config.base_url.as_deref().unwrap_or("(default)"),
        persist_enabled = config.persist.enabled,
        data_dir = %config.persist.data_dir,
        instance_id = %instance_id,
        "andromeda configured (api_key not logged)"
    );

    let addr: SocketAddr = match config.listen.parse() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("invalid listen address {}: {err}", config.listen);
            return ExitCode::from(1);
        }
    };

    let llm: Arc<dyn andromeda::LlmPort> = match LiterAdapter::from_config(&config) {
        Ok(adapter) => Arc::new(adapter),
        Err(err) => {
            eprintln!("failed to create LLM client: {err}");
            return ExitCode::from(1);
        }
    };

    let store: Option<Arc<dyn RunStore>> = if config.persist.enabled {
        Some(Arc::new(LocalFsRunStore::new(
            config.persist.data_dir.clone(),
        )))
    } else {
        None
    };

    let state = AppState {
        registry: RunRegistry::new(),
        store,
        instance_id,
        persist_enabled: config.persist.enabled,
        llm,
        follow_up: policy_from_name(&config.follow_up_policy),
        tool_timeout: Duration::from_secs(config.tool_timeout_secs),
        middlewares: default_summarize_chain(config.context.clone()),
        guards: config.guards.clone(),
        subagents: config.subagents.clone(),
    };

    let app = router(state).layer(TraceLayer::new_for_http());
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!("failed to bind {addr}: {err}");
            return ExitCode::from(1);
        }
    };

    tracing::info!("listening on {addr}");

    if let Err(err) = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await
    {
        tracing::error!("server error: {err}");
        return ExitCode::from(1);
    }

    tracing::info!("shutting down");
    ExitCode::SUCCESS
}
