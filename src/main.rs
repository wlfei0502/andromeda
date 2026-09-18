use std::path::Path;
use std::process::ExitCode;

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
    tracing::info!(
        "will listen on {} once HTTP is wired (Task 5)",
        config.listen
    );

    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!("failed to listen for ctrl-c: {err}");
        return ExitCode::from(1);
    }

    tracing::info!("shutting down");
    ExitCode::SUCCESS
}
