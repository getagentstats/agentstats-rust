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
