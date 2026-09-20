//! Durable run checkpoint store (cold state).
//!
//! Depends on: `protocol` only.
//! Used by: `agent` / `api` / `runtime` (later milestones).

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{TodoItem, ToolDef, WireMessage};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    WaitingTool,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::WaitingTool => "waiting_tool",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingTool {
    pub tool_call_id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GuardsSnapshot {
    pub llm_rounds: u32,
    pub follow_up_rounds: u32,
    pub started_at: String,
    pub updated_at: String,
}

impl GuardsSnapshot {
    pub fn new_now() -> Self {
        let now = now_rfc3339();
        Self {
            llm_rounds: 0,
            follow_up_rounds: 0,
            started_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = now_rfc3339();
    }
}

fn now_rfc3339() -> String {
    // Avoid pulling chrono for M1: UNIX seconds is enough for ordering/debug.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{secs}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    pub run_id: String,
    pub status: RunStatus,
    pub context: Vec<WireMessage>,
    pub tools: Vec<ToolDef>,
    #[serde(default)]
    pub todos: Vec<TodoItem>,
    #[serde(default)]
    pub plan_mode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_tool: Option<PendingTool>,
    pub guards: GuardsSnapshot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    pub revision: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("checkpoint revision conflict")]
    Conflict,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[async_trait]
pub trait RunStore: Send + Sync {
    async fn save(&self, cp: &Checkpoint) -> Result<(), StoreError>;

    /// Write only if the on-disk revision equals `expected_revision`.
    /// Missing file is treated as revision `0`.
    async fn save_cas(
        &self,
        cp: &Checkpoint,
        expected_revision: u64,
    ) -> Result<(), StoreError>;

    async fn load(&self, run_id: &str) -> Result<Option<Checkpoint>, StoreError>;

    async fn write_meta(&self, run_id: &str, meta: &Value) -> Result<(), StoreError>;
}

#[derive(Debug, Clone)]
pub struct LocalFsRunStore {
    root: PathBuf,
}

impl LocalFsRunStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: data_dir.into(),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn run_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("runs").join(run_id)
    }

    fn checkpoint_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("checkpoint.json")
    }

    fn meta_path(&self, run_id: &str) -> PathBuf {
        self.run_dir(run_id).join("meta.json")
    }

    fn load_revision_sync(&self, run_id: &str) -> Result<u64, StoreError> {
        match self.load_sync(run_id)? {
            Some(cp) => Ok(cp.revision),
            None => Ok(0),
        }
    }

    fn load_sync(&self, run_id: &str) -> Result<Option<Checkpoint>, StoreError> {
        let path = self.checkpoint_path(run_id);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)?;
        let cp: Checkpoint = serde_json::from_str(&text)?;
        Ok(Some(cp))
    }

    fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<(), StoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(value)?;
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    fn save_sync(&self, cp: &Checkpoint) -> Result<(), StoreError> {
        Self::write_json_atomic(&self.checkpoint_path(&cp.run_id), cp)
    }
}

#[async_trait]
impl RunStore for LocalFsRunStore {
    async fn save(&self, cp: &Checkpoint) -> Result<(), StoreError> {
        self.save_sync(cp)
    }

    async fn save_cas(
        &self,
        cp: &Checkpoint,
        expected_revision: u64,
    ) -> Result<(), StoreError> {
        let current = self.load_revision_sync(&cp.run_id)?;
        if current != expected_revision {
            return Err(StoreError::Conflict);
        }
        self.save_sync(cp)
    }

    async fn load(&self, run_id: &str) -> Result<Option<Checkpoint>, StoreError> {
        self.load_sync(run_id)
    }

    async fn write_meta(&self, run_id: &str, meta: &Value) -> Result<(), StoreError> {
        Self::write_json_atomic(&self.meta_path(run_id), meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Role, WireMessage};
    use serde_json::json;

    fn sample_checkpoint(run_id: &str, revision: u64) -> Checkpoint {
        Checkpoint {
            run_id: run_id.into(),
            status: RunStatus::WaitingTool,
            context: vec![WireMessage {
                role: Role::User,
                content: "hi".into(),
                tool_call_id: None,
                name: None,
                tool_calls: None,
            reasoning_content: None,
        }],
            tools: vec![],
            todos: vec![],
            plan_mode: false,
            pending_tool: Some(PendingTool {
                tool_call_id: "call_1".into(),
                name: "echo".into(),
                arguments: json!({"text": "x"}),
            }),
            guards: GuardsSnapshot::new_now(),
            parent_run_id: None,
            owner_id: Some("node-a".into()),
            revision,
        }
    }

    #[test]
    fn checkpoint_deserializes_legacy_without_plan_mode() {
        let v = json!({
            "run_id": "r1",
            "status": "running",
            "context": [],
            "tools": [],
            "todos": [{
                "id": "t1",
                "content": "a",
                "status": "pending"
            }],
            "guards": {
                "llm_rounds": 0,
                "follow_up_rounds": 0,
                "started_at": "0",
                "updated_at": "0"
            },
            "revision": 1
        });
        let cp: Checkpoint = serde_json::from_value(v).unwrap();
        assert!(!cp.plan_mode);
        assert_eq!(cp.todos.len(), 1);
        assert_eq!(cp.todos[0].id, "t1");
        assert_eq!(cp.todos[0].status, crate::protocol::TodoStatus::Pending);
    }

    #[tokio::test]
    async fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsRunStore::new(dir.path());
        let cp = sample_checkpoint("r1", 1);
        store.save(&cp).await.unwrap();

        let loaded = store.load("r1").await.unwrap().expect("present");
        assert_eq!(loaded.run_id, "r1");
        assert_eq!(loaded.revision, 1);
        assert_eq!(loaded.status, RunStatus::WaitingTool);
        assert_eq!(
            loaded.pending_tool.as_ref().unwrap().tool_call_id,
            "call_1"
        );
        assert_eq!(loaded.context[0].content, "hi");
        assert_eq!(loaded.owner_id.as_deref(), Some("node-a"));
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsRunStore::new(dir.path());
        assert!(store.load("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_cas_succeeds_then_conflicts_on_stale() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsRunStore::new(dir.path());

        let mut cp = sample_checkpoint("r2", 1);
        store.save_cas(&cp, 0).await.unwrap();

        cp.revision = 2;
        cp.status = RunStatus::Running;
        cp.pending_tool = None;
        store.save_cas(&cp, 1).await.unwrap();

        let stale = sample_checkpoint("r2", 2);
        let err = store.save_cas(&stale, 1).await.unwrap_err();
        assert!(matches!(err, StoreError::Conflict));

        let loaded = store.load("r2").await.unwrap().unwrap();
        assert_eq!(loaded.revision, 2);
        assert_eq!(loaded.status, RunStatus::Running);
    }

    #[tokio::test]
    async fn write_meta_creates_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsRunStore::new(dir.path());
        store
            .write_meta("r3", &json!({"created": true}))
            .await
            .unwrap();
        let text = std::fs::read_to_string(dir.path().join("runs/r3/meta.json")).unwrap();
        assert!(text.contains("created"));
    }
}
