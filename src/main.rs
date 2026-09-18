use std::net::SocketAddr;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use andromeda::follow_up::policy_from_name;
use andromeda::http::{AppState, router};
use andromeda::llm::LiterAdapter;
use andromeda::run::RunRegistry;
use tower_http::trace::TraceLayer;

const CONFIG_PATH: &str = "config.toml";

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = match andromeda::AppConfig::load(Path::new(CONFIG_PATH)) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };

    tracing::info!(
        model = %config.model,
        listen = %config.listen,
        tool_timeout_secs = config.tool_timeout_secs,
        follow_up_policy = %config.follow_up_policy,
        base_url = config.base_url.as_deref().unwrap_or("(default)"),
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

    let state = AppState {
        registry: RunRegistry::new(),
        llm,
        follow_up: policy_from_name(&config.follow_up_policy),
        tool_timeout: Duration::from_secs(config.tool_timeout_secs),
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
