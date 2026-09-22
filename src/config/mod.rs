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
    /// tracing EnvFilter directive, e.g. `info` or `andromeda=debug,tower_http=info`
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// Checkpoint / resume store. TOML key `[persist]` (alias `[long_horizon]` still accepted).
    #[serde(default, alias = "long_horizon")]
    pub persist: PersistConfig,
    #[serde(default)]
    pub context: ContextConfig,
    #[serde(default)]
    pub guards: GuardsConfig,
    #[serde(default)]
    pub subagents: SubagentsConfig,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SubagentsConfig {
    #[serde(default = "default_max_concurrent_subagents")]
    pub max_concurrent_subagents: u32,
    #[serde(default = "default_subagent_timeout_secs")]
    pub subagent_timeout_secs: u64,
}

impl Default for SubagentsConfig {
    fn default() -> Self {
        Self {
            max_concurrent_subagents: default_max_concurrent_subagents(),
            subagent_timeout_secs: default_subagent_timeout_secs(),
        }
    }
}

fn default_max_concurrent_subagents() -> u32 {
    2
}

fn default_subagent_timeout_secs() -> u64 {
    900
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ContextConfig {
    #[serde(default = "default_summarize_threshold_tokens")]
    pub summarize_threshold_tokens: u64,
    #[serde(default = "default_keep_last_messages")]
    pub keep_last_messages: usize,
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: u64,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            summarize_threshold_tokens: default_summarize_threshold_tokens(),
            keep_last_messages: default_keep_last_messages(),
            max_context_tokens: default_max_context_tokens(),
        }
    }
}

fn default_summarize_threshold_tokens() -> u64 {
    80_000
}

fn default_keep_last_messages() -> usize {
    24
}

fn default_max_context_tokens() -> u64 {
    120_000
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GuardsConfig {
    /// `0` disables the LLM-round cap.
    #[serde(default = "default_max_llm_rounds")]
    pub max_llm_rounds: u32,
    /// `0` disables the wall-clock cap. Counted from `guards.started_at`.
    #[serde(default = "default_max_run_wall_secs")]
    pub max_run_wall_secs: u64,
    /// `0` disables the follow-up cap.
    #[serde(default = "default_max_follow_up_rounds")]
    pub max_follow_up_rounds: u32,
    /// `0` disables no-progress detection.
    #[serde(default = "default_max_noop_llm_rounds")]
    pub max_noop_llm_rounds: u32,
}

impl Default for GuardsConfig {
    fn default() -> Self {
        Self {
            max_llm_rounds: default_max_llm_rounds(),
            max_run_wall_secs: default_max_run_wall_secs(),
            max_follow_up_rounds: default_max_follow_up_rounds(),
            max_noop_llm_rounds: default_max_noop_llm_rounds(),
        }
    }
}

fn default_max_llm_rounds() -> u32 {
    200
}

fn default_max_run_wall_secs() -> u64 {
    7200
}

fn default_max_follow_up_rounds() -> u32 {
    8
}

fn default_max_noop_llm_rounds() -> u32 {
    5
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PersistConfig {
    /// When false, RunStore is not created; runs stay memory-only.
    #[serde(default = "default_persist_enabled")]
    pub enabled: bool,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Empty → generate a UUID at process startup and keep it on `AppState`.
    #[serde(default)]
    pub instance_id: String,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            enabled: default_persist_enabled(),
            data_dir: default_data_dir(),
            instance_id: String::new(),
        }
    }
}

fn default_persist_enabled() -> bool {
    true
}

fn default_data_dir() -> String {
    "./data".into()
}

fn default_model() -> String {
    "deepseek-v4-flash-0731".into()
}

fn default_listen() -> String {
    "127.0.0.1:8082".into()
}

fn default_tool_timeout_secs() -> u64 {
    60
}

fn default_follow_up_policy() -> String {
    "noop".into()
}

fn default_log_level() -> String {
    "info".into()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_defaults_when_section_omitted() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk-test"
            model = "m"
            "#,
        )
        .unwrap();
        assert!(cfg.persist.enabled);
        assert_eq!(cfg.persist.data_dir, "./data");
        assert!(cfg.persist.instance_id.is_empty());
    }

    #[test]
    fn persist_section_overrides() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk-test"
            [persist]
            enabled = false
            data_dir = "/tmp/andromeda-data"
            instance_id = "node-a"
            "#,
        )
        .unwrap();
        assert!(!cfg.persist.enabled);
        assert_eq!(cfg.persist.data_dir, "/tmp/andromeda-data");
        assert_eq!(cfg.persist.instance_id, "node-a");
    }

    #[test]
    fn context_defaults_when_section_omitted() {
        let cfg: AppConfig = toml::from_str(r#"api_key = "sk""#).unwrap();
        assert_eq!(cfg.context.summarize_threshold_tokens, 80_000);
        assert_eq!(cfg.context.keep_last_messages, 24);
        assert_eq!(cfg.context.max_context_tokens, 120_000);
    }

    #[test]
    fn context_section_overrides() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk"
            [context]
            summarize_threshold_tokens = 100
            keep_last_messages = 4
            max_context_tokens = 200
            "#,
        )
        .unwrap();
        assert_eq!(cfg.context.summarize_threshold_tokens, 100);
        assert_eq!(cfg.context.keep_last_messages, 4);
        assert_eq!(cfg.context.max_context_tokens, 200);
    }

    #[test]
    fn long_horizon_alias_still_loads() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk-test"
            [long_horizon]
            enabled = false
            data_dir = "/tmp/old"
            "#,
        )
        .unwrap();
        assert!(!cfg.persist.enabled);
        assert_eq!(cfg.persist.data_dir, "/tmp/old");
    }

    #[test]
    fn guards_defaults_when_section_omitted() {
        let cfg: AppConfig = toml::from_str(r#"api_key = "sk""#).unwrap();
        assert_eq!(cfg.guards.max_llm_rounds, 200);
        assert_eq!(cfg.guards.max_run_wall_secs, 7200);
        assert_eq!(cfg.guards.max_follow_up_rounds, 8);
        assert_eq!(cfg.guards.max_noop_llm_rounds, 5);
    }

    #[test]
    fn guards_zero_disables() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk"
            [guards]
            max_llm_rounds = 0
            max_run_wall_secs = 0
            max_follow_up_rounds = 0
            max_noop_llm_rounds = 0
            "#,
        )
        .unwrap();
        assert_eq!(
            cfg.guards,
            GuardsConfig {
                max_llm_rounds: 0,
                max_run_wall_secs: 0,
                max_follow_up_rounds: 0,
                max_noop_llm_rounds: 0,
            }
        );
    }

    #[test]
    fn subagents_defaults_when_section_omitted() {
        let cfg: AppConfig = toml::from_str(r#"api_key = "sk""#).unwrap();
        assert_eq!(cfg.subagents.max_concurrent_subagents, 2);
        assert_eq!(cfg.subagents.subagent_timeout_secs, 900);
    }

    #[test]
    fn subagents_section_overrides() {
        let cfg: AppConfig = toml::from_str(
            r#"
            api_key = "sk"
            [subagents]
            max_concurrent_subagents = 1
            subagent_timeout_secs = 30
            "#,
        )
        .unwrap();
        assert_eq!(cfg.subagents.max_concurrent_subagents, 1);
        assert_eq!(cfg.subagents.subagent_timeout_secs, 30);
    }
}
