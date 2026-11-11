//! AgentStats analytics for Rust.
//!
//! - MCP servers built on [`rmcp`](https://docs.rs/rmcp): [`AgentStats`] (feature `mcp`, on by default).
//! - Command-line tools: [`cli`], with a one-call integration for clap (feature `clap`).
//!   A CLI that doesn't run an MCP server can leave out rmcp:
//!   `agentstats = { version = "0.1", default-features = false, features = ["clap"] }`.

#[cfg(feature = "mcp")]
mod mcp;
#[cfg(feature = "mcp")]
pub use mcp::*;

pub mod cli;

/// Where events go: AgentStats' ingest URL for one kind of data ("mcp", "cli"). It isn't an
/// option; `AGENTSTATS_URL` (undocumented) is for AgentStats' own development and tests.
pub(crate) fn ingest_url(kind: &str) -> String {
    let base = std::env::var("AGENTSTATS_URL")
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://agentstats.io".into());
    format!("{}/api/ingest/{kind}", base.trim_end_matches('/'))
}
