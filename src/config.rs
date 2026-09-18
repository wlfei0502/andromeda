use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub api_key: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_tool_timeout_secs")]
    pub tool_timeout_secs: u64,
    #[serde(default = "default_follow_up_policy")]
    pub follow_up_policy: String,
}

fn default_model() -> String {
    "gpt-4o-mini".into()
}

fn default_listen() -> String {
    "127.0.0.1:8080".into()
}

fn default_tool_timeout_secs() -> u64 {
    60
}

fn default_follow_up_policy() -> String {
    "noop".into()
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|err| {
            format!(
                "failed to read {}: {err}\nCopy config.example.toml to config.toml and fill in api_key.",
                path.display()
            )
        })?;
        let config: AppConfig = toml::from_str(&text)
            .map_err(|err| format!("failed to parse {}: {err}", path.display()))?;
        if config.api_key.trim().is_empty() {
            return Err(format!("{}: api_key is required", path.display()));
        }
        Ok(config)
    }
}
