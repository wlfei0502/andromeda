//! Long-run guard checks (LH-M4). Pure helpers; the orchestrator applies them.

use std::collections::HashSet;

use crate::config::GuardsConfig;
use crate::store::GuardsSnapshot;

pub const REASON_LLM_ROUNDS: &str = "guard_llm_rounds";
pub const REASON_TIMEOUT: &str = "guard_timeout";
pub const REASON_NOOP: &str = "guard_noop";

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Limits that should stop the run before the next LLM call.
pub fn pre_llm_guard(
    guards: &GuardsSnapshot,
    cfg: &GuardsConfig,
    now: u64,
) -> Option<&'static str> {
    if wall_exceeded(guards, cfg, now) {
        return Some(REASON_TIMEOUT);
    }
    if cfg.max_llm_rounds > 0 && guards.llm_rounds >= cfg.max_llm_rounds {
        return Some(REASON_LLM_ROUNDS);
    }
    None
}

pub fn wall_exceeded(guards: &GuardsSnapshot, cfg: &GuardsConfig, now: u64) -> bool {
    if cfg.max_run_wall_secs == 0 {
        return false;
    }
    let Some(started) = guards.started_at.parse::<u64>().ok() else {
        return false;
    };
    now.saturating_sub(started) >= cfg.max_run_wall_secs
}

pub fn normalize_text(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

pub fn highly_similar(a: &str, b: &str) -> bool {
    let a = normalize_text(a);
    let b = normalize_text(b);
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if a == b {
        return true;
    }
    let (short, long) = if a.len() <= b.len() {
        (&a, &b)
    } else {
        (&b, &a)
    };
    if long.contains(short.as_str())
        && short.len().saturating_mul(5) >= long.len().saturating_mul(4)
    {
        return true;
    }
    bigram_jaccard(&a, &b) >= 0.85
}

/// Record a no-tool assistant turn. Returns `guard_noop` when the streak reaches the cap.
///
/// The first turn only stores a baseline (`noop_streak` stays 0). Each later turn that is
/// highly similar to that baseline increments the streak.
pub fn observe_noop_turn(
    guards: &mut GuardsSnapshot,
    assistant_text: &str,
    cfg: &GuardsConfig,
) -> Option<&'static str> {
    let norm = normalize_text(assistant_text);
    if cfg.max_noop_llm_rounds > 0 && highly_similar(assistant_text, &guards.last_assistant_norm) {
        guards.noop_streak = guards.noop_streak.saturating_add(1);
    } else {
        guards.noop_streak = 0;
    }
    guards.last_assistant_norm = norm;
    if cfg.max_noop_llm_rounds > 0 && guards.noop_streak >= cfg.max_noop_llm_rounds {
        Some(REASON_NOOP)
    } else {
        None
    }
}

pub fn clear_noop_progress(guards: &mut GuardsSnapshot) {
    guards.noop_streak = 0;
    guards.last_assistant_norm.clear();
}

fn bigram_jaccard(a: &str, b: &str) -> f64 {
    let ba = bigrams(a);
    let bb = bigrams(b);
    if ba.is_empty() || bb.is_empty() {
        return 0.0;
    }
    let inter = ba.intersection(&bb).count();
    let union = ba.union(&bb).count();
    if union == 0 {
        0.0
    } else {
        inter as f64 / union as f64
    }
}

fn bigrams(text: &str) -> HashSet<(char, char)> {
    let chars: Vec<char> = text.chars().collect();
    chars.windows(2).map(|w| (w[0], w[1])).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(rounds: u32, wall: u64, noop: u32) -> GuardsConfig {
        GuardsConfig {
            max_llm_rounds: rounds,
            max_run_wall_secs: wall,
            max_follow_up_rounds: 8,
            max_noop_llm_rounds: noop,
        }
    }

    fn snap(rounds: u32, started: &str) -> GuardsSnapshot {
        GuardsSnapshot {
            llm_rounds: rounds,
            follow_up_rounds: 0,
            started_at: started.into(),
            updated_at: started.into(),
            noop_streak: 0,
            last_assistant_norm: String::new(),
        }
    }

    #[test]
    fn pre_llm_trips_rounds_then_wall_first() {
        let g = snap(2, "100");
        assert_eq!(
            pre_llm_guard(&g, &cfg(2, 0, 0), 100),
            Some(REASON_LLM_ROUNDS)
        );
        assert_eq!(pre_llm_guard(&g, &cfg(0, 0, 0), 1000), None);
        assert_eq!(
            pre_llm_guard(&g, &cfg(99, 50, 0), 160),
            Some(REASON_TIMEOUT)
        );
        assert!(!wall_exceeded(&g, &cfg(0, 50, 0), 149));
        assert!(wall_exceeded(&g, &cfg(0, 50, 0), 150));
    }

    #[test]
    fn similar_text_ignores_space_and_case() {
        assert!(highly_similar("Hello   World", "hello world"));
        assert!(!highly_similar("ship the crate", "draw a map"));
        assert!(!highly_similar("", "hello"));
    }

    #[test]
    fn noop_streak_counts_repeats_after_baseline() {
        let mut g = snap(0, "1");
        let c = cfg(0, 0, 2);
        assert_eq!(observe_noop_turn(&mut g, "still working", &c), None);
        assert_eq!(g.noop_streak, 0);
        assert_eq!(observe_noop_turn(&mut g, "still working", &c), None);
        assert_eq!(g.noop_streak, 1);
        assert_eq!(
            observe_noop_turn(&mut g, "Still   working", &c),
            Some(REASON_NOOP)
        );
        clear_noop_progress(&mut g);
        assert_eq!(g.noop_streak, 0);
        assert!(g.last_assistant_norm.is_empty());
        assert_eq!(
            observe_noop_turn(&mut g, "still working", &cfg(0, 0, 0)),
            None
        );
    }
}
